#include "io.hpp"

#include <errno.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <chrono>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <assert.h>
#include <condition_variable>
#include <mutex>
#include <unordered_map>

ReadAheadStoragePool::Storage::Storage(Storage&& other) noexcept
    : fd(std::move(other.fd)),
      mapping(std::exchange(other.mapping, nullptr)),
      size(std::exchange(other.size, 0)), charge(std::move(other.charge)) {}

ReadAheadStoragePool::Storage& ReadAheadStoragePool::Storage::operator=(
    Storage&& other) noexcept {
  if (this != &other) {
    unmap();
    fd      = std::move(other.fd);
    // The old fd is closed before its reservation wakes another allocator.
    charge  = std::move(other.charge);
    mapping = std::exchange(other.mapping, nullptr);
    size    = std::exchange(other.size, 0);
  }
  return *this;
}

ReadAheadStoragePool::Storage::~Storage() {
  unmap();
  fd.reset();
  charge.reset();
}

void ReadAheadStoragePool::Storage::unmap() noexcept {
  if (mapping != nullptr) {
    ::munmap(mapping, size);
    mapping = nullptr;
  }
}

bool ReadAheadStoragePool::Storage::discard(size_t offset, size_t length) noexcept {
  static const size_t page = size_t(::sysconf(_SC_PAGESIZE));
  if (!fd || length == 0 || offset % page || length % page ||
      offset > size || length > size - offset ||
      (charge && (length > charge.bytes() || length % charge.page_size()))) {
    errno = EINVAL;
    return false;
  }
  if (::fallocate(fd.get(), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                  off_t(offset), off_t(length)) != 0) return false;
  if (charge) charge.split(length).reset();
  return true;
}

ReadAheadStoragePool::ReadAheadStoragePool(size_t max_idle_fds)
    : max_idle_fds_(max_idle_fds) {
  entries_.reserve(max_idle_fds);
}

ReadAheadStoragePool::Storage ReadAheadStoragePool::acquire(size_t size) {
  return acquire(size, {});
}

ReadAheadStoragePool::Storage ReadAheadStoragePool::acquire(
    size_t size, PrefetchBudget::Reservation charge) {
  if (size == 0 || size > uint64_t(INT64_MAX)) {
    throw std::invalid_argument("invalid read-ahead storage size");
  }
  if (charge && size > charge.bytes()) {
    throw std::invalid_argument("read-ahead storage exceeds reservation");
  }
  Storage storage;
  storage.charge = std::move(charge);
  {
    std::lock_guard guard(mutex_);
    if (!entries_.empty()) {
      storage.fd = std::move(entries_.back());
      entries_.pop_back();
    }
  }
  if (!storage.fd) {
    storage.fd.reset(::memfd_create("ngs3fs-read-ahead", MFD_CLOEXEC));
    if (!storage.fd) {
      throw std::system_error(errno, std::generic_category(),
                              "memfd_create(read-ahead)");
    }
  }
  if (::ftruncate(storage.fd.get(), off_t(size)) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "ftruncate(read-ahead)");
  }
  storage.size = size;
  storage.mapping = ::mmap(nullptr, size, PROT_READ, MAP_SHARED,
                           storage.fd.get(), 0);
  if (storage.mapping == MAP_FAILED) {
    storage.mapping = nullptr;
    throw std::system_error(errno, std::generic_category(), "mmap(read-ahead)");
  }
  return storage;
}

void ReadAheadStoragePool::release(Storage storage) noexcept {
  if (!storage.fd || max_idle_fds_ == 0) return;
  storage.unmap();
  // Truncate outside the pool lock. Other owners must have retired before
  // release; pipe references already handed off retain their own page refs.
  if (::ftruncate(storage.fd.get(), 0) != 0) return;
  {
    std::lock_guard guard(mutex_);
    if (entries_.size() < max_idle_fds_) {
      entries_.push_back(std::move(storage.fd)); // Capacity reserved at construction.
    }
  }
  // Storage's destructor releases its charge outside the pool lock, after
  // truncation (or close). Budget callbacks may immediately reuse this pool.
}

struct PrefetchBudget::State {
  struct Usage {
    size_t speculative = 0;
    size_t demand      = 0;
  };
  State(size_t cap, size_t reserve, size_t page)
      : capacity(cap), demand_reserve(reserve), page_size(page) {
    files.reserve(16);
  }
  ~State() { if (event >= 0) ::close(event); }
  int event = -1;
  unsigned event_waiters = 0;

  void wake_event() noexcept {
    std::lock_guard guard(mutex);
    if (event < 0 || event_waiters == 0) return;
    const uint64_t n = event_waiters;
    ssize_t rc;
    do { rc = ::write(event, &n, sizeof(n)); } while (rc < 0 && errno == EINTR);
  }

  std::mutex mutex;
  std::condition_variable condition;
  std::unordered_map<uintptr_t, Usage> files;
  size_t capacity;
  size_t demand_reserve;
  size_t page_size;
  Usage used;
  size_t peak = 0;
  size_t file_peak = 0;
  uint64_t revision = 0;
  bool stopped = false;
  Waiter* waiters = nullptr;

  Waiter* detach_waiters() noexcept {
    Waiter* list = std::exchange(waiters, nullptr);
    for (Waiter* p = list; p != nullptr; p = p->next) p->linked = false;
    return list;
  }
  static void wake(Waiter* list) noexcept {
    while (list != nullptr) {
      Waiter* p = list;
      list = p->next;
      p->next = nullptr;
      p->ready(p->context);
    }
  }
};

PrefetchBudget::PrefetchBudget(size_t capacity, size_t reserve, size_t page) {
  if (page == 0 || reserve == 0 || capacity < reserve ||
      capacity % page != 0 || reserve % page != 0) {
    throw std::invalid_argument("invalid prefetch memory budget");
  }
  state_ = std::make_shared<State>(capacity, reserve, page);
}

size_t PrefetchBudget::default_capacity(uint64_t physical_bytes, size_t page) {
  if (page == 0) throw std::invalid_argument("invalid page size");
  const uint64_t limit = physical_bytes / 10;
  if (limit > SIZE_MAX) throw std::overflow_error("prefetch budget exceeds SIZE_MAX");
  return size_t(limit - limit % page);
}

size_t PrefetchBudget::file_limit(uint64_t size, size_t window,
                                 size_t configured, size_t page) {
  if (page == 0 || window == 0 || window % page != 0 || configured % page != 0) {
    throw std::invalid_argument("prefetch limits must be page aligned");
  }
  const size_t maximum = SIZE_MAX - SIZE_MAX % page;
  const size_t adaptive = window > maximum / 2 ? maximum : window * 2;
  const uint64_t limit = configured ? configured : std::min<uint64_t>(size, adaptive);
  if (limit > maximum) throw std::overflow_error("file prefetch budget exceeds SIZE_MAX");
  const size_t remainder = size_t(limit % page);
  return size_t(limit) + (remainder == 0 ? 0 : page - remainder);
}

PrefetchBudget::Reservation PrefetchBudget::try_reserve(
    uintptr_t file, size_t limit, size_t preferred, size_t minimum, bool demand) {
  State& s = *state_;
  if (minimum == 0 || minimum > preferred || limit == 0 ||
      limit % s.page_size != 0 || preferred % s.page_size != 0 ||
      minimum % s.page_size != 0) {
    throw std::invalid_argument("prefetch reservation must be page aligned");
  }
  std::lock_guard guard(s.mutex);
  if (s.stopped) return {};
  const auto found = s.files.find(file);
  const State::Usage used = found == s.files.end() ? State::Usage{} : found->second;
  const size_t file_reserve = std::min(limit, s.demand_reserve);
  const size_t cap = demand ? s.demand_reserve : s.capacity - s.demand_reserve;
  const size_t file_cap = demand ? file_reserve : limit - file_reserve;
  const size_t taken = demand ? s.used.demand : s.used.speculative;
  const size_t file_taken = demand ? used.demand : used.speculative;
  const size_t total = used.demand + used.speculative;
  if (taken > cap || file_taken > file_cap || total > limit) return {};
  const size_t bytes = std::min({preferred, cap - taken, file_cap - file_taken,
                                  limit - total});
  if (bytes < minimum) return {};
  // Insertion can throw: do it before changing any counters.
  auto& entry = s.files.try_emplace(file).first->second;
  (demand ? entry.demand : entry.speculative) += bytes;
  (demand ? s.used.demand : s.used.speculative) += bytes;
  s.peak = std::max(s.peak, s.used.demand + s.used.speculative);
  s.file_peak = std::max(s.file_peak, entry.demand + entry.speculative);
  assert(s.used.demand + s.used.speculative <= s.capacity);
  return Reservation(state_, file, bytes, demand);
}

PrefetchBudget::Reservation::Reservation(Reservation&& other) noexcept
    : state_(std::move(other.state_)), file_(other.file_),
      bytes_(std::exchange(other.bytes_, 0)), demand_(other.demand_) {}

PrefetchBudget::Reservation PrefetchBudget::Reservation::split(size_t bytes) {
  if (!state_ || bytes == 0 || bytes > bytes_ || bytes % state_->page_size != 0) {
    throw std::invalid_argument("invalid prefetch reservation split");
  }
  bytes_ -= bytes;
  return Reservation(state_, file_, bytes, demand_);
}

size_t PrefetchBudget::Reservation::page_size() const noexcept {
  return state_ ? state_->page_size : 0;
}

PrefetchBudget::Reservation& PrefetchBudget::Reservation::operator=(
    Reservation&& other) noexcept {
  if (this != &other) {
    reset();
    state_  = std::move(other.state_);
    file_   = other.file_;
    bytes_  = std::exchange(other.bytes_, 0);
    demand_ = other.demand_;
  }
  return *this;
}

void PrefetchBudget::Reservation::reset() noexcept {
  auto state = std::move(state_);
  const size_t bytes = std::exchange(bytes_, 0);
  if (bytes == 0) return;
  Waiter* ready;
  {
    std::lock_guard guard(state->mutex);
    auto found = state->files.find(file_);
    assert(found != state->files.end());
    auto& entry = found->second;
    size_t& file_bytes = demand_ ? entry.demand : entry.speculative;
    size_t& all_bytes = demand_ ? state->used.demand : state->used.speculative;
    assert(file_bytes >= bytes && all_bytes >= bytes);
    file_bytes -= bytes;
    all_bytes -= bytes;
    if (entry.demand == 0 && entry.speculative == 0) state->files.erase(found);
    ++state->revision;
    ready = state->detach_waiters();
  }
  state->condition.notify_all();
  state->wake_event();
  State::wake(ready);
}

int PrefetchBudget::begin_async_wait(uint64_t revision) {
  std::lock_guard guard(state_->mutex);
  if (state_->stopped || state_->revision != revision) return -1;
  if (state_->event < 0) {
    state_->event = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK | EFD_SEMAPHORE);
    if (state_->event < 0) throw std::system_error(errno, std::generic_category(), "prefetch budget eventfd");
  }
  ++state_->event_waiters;
  return state_->event;
}

void PrefetchBudget::end_async_wait() noexcept {
  std::lock_guard guard(state_->mutex);
  assert(state_->event_waiters != 0);
  --state_->event_waiters;
}

PrefetchBudget::Snapshot PrefetchBudget::snapshot() const noexcept {
  std::lock_guard guard(state_->mutex);
  return {state_->used.demand + state_->used.speculative, state_->peak,
          state_->files.size(), state_->revision, state_->stopped, state_->file_peak};
}

size_t PrefetchBudget::file_used(uintptr_t file) const noexcept {
  std::lock_guard guard(state_->mutex);
  const auto found = state_->files.find(file);
  return found == state_->files.end() ? 0
      : found->second.demand + found->second.speculative;
}

bool PrefetchBudget::subscribe(Waiter& waiter, uint64_t revision) {
  std::lock_guard guard(state_->mutex);
  if (waiter.ready == nullptr || waiter.linked) {
    throw std::invalid_argument("invalid prefetch budget waiter");
  }
  if (state_->stopped || state_->revision != revision) return false;
  waiter.next = state_->waiters;
  waiter.linked = true;
  state_->waiters = &waiter;
  return true;
}

bool PrefetchBudget::unsubscribe(Waiter& waiter) noexcept {
  std::lock_guard guard(state_->mutex);
  Waiter** p = &state_->waiters;
  while (*p != nullptr && *p != &waiter) p = &(*p)->next;
  if (*p == nullptr) return false; // A detached waiter still owns its callback.
  *p = waiter.next;
  waiter.next = nullptr;
  waiter.linked = false;
  return true;
}

bool PrefetchBudget::wait(uint64_t revision) {
  std::unique_lock guard(state_->mutex);
  state_->condition.wait(guard, [&] {
    return state_->stopped || state_->revision != revision;
  });
  return !state_->stopped;
}

void PrefetchBudget::stop() noexcept {
  Waiter* ready;
  {
    std::lock_guard guard(state_->mutex);
    if (state_->stopped) return;
    state_->stopped = true;
    ++state_->revision;
    ready = state_->detach_waiters();
  }
  state_->condition.notify_all();
  state_->wake_event();
  State::wake(ready);
}

IoMutex::~IoMutex() {
  const int fd = event_.load(std::memory_order_relaxed);
  if (fd >= 0) ::close(fd);
}

int IoMutex::begin_async_wait() {
  int fd = event_.load(std::memory_order_acquire);
  if (fd < 0) {
    UniqueFd candidate(::eventfd(0, EFD_CLOEXEC | EFD_SEMAPHORE));
    if (!candidate) {
      throw std::system_error(errno, std::generic_category(), "eventfd(directory mutex)");
    }
    fd = -1;
    if (event_.compare_exchange_strong(fd, candidate.get(),
            std::memory_order_release, std::memory_order_acquire)) {
      fd = candidate.release();
    }
  }
  waiters_.fetch_add(1, std::memory_order_acq_rel);
  return fd;
}

void IoMutex::unlock() noexcept {
  locked_.store(false, std::memory_order_release);
  locked_.notify_one();
  const uint64_t waiters = waiters_.load(std::memory_order_acquire);
  if (waiters == 0) return;
  const int saved_errno = errno;
  const int fd = event_.load(std::memory_order_acquire);
  ssize_t result;
  do {
    result = ::write(fd, &waiters, sizeof(waiters));
  } while (result < 0 && errno == EINTR);
  errno = saved_errno;
}

thread_local IoExecutor* current_io_executor = nullptr;
thread_local int current_io_timeout_ms       = 0;

IoExecutor* io_executor() noexcept {
  return current_io_executor;
}

bool IoExecutor::submit(AsyncIoRequest&) noexcept {
  errno = ENOTSUP;
  return false;
}

bool IoExecutor::cancel(AsyncIoRequest&) noexcept {
  errno = ENOTSUP;
  return false;
}

IoExecutorScope::IoExecutorScope(IoExecutor* executor,
                                 int timeout_ms) noexcept
    : previous_executor_(current_io_executor),
      previous_timeout_ms_(current_io_timeout_ms) {
  current_io_executor   = executor;
  current_io_timeout_ms = timeout_ms;
}

IoExecutorScope::~IoExecutorScope() {
  current_io_executor   = previous_executor_;
  current_io_timeout_ms = previous_timeout_ms_;
}

int effective_io_timeout(int timeout_ms) noexcept {
  return timeout_ms > 0 ? timeout_ms : current_io_timeout_ms;
}

ssize_t io_receive(int fd, void* data, size_t length, int flags,
                   int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::recv(fd, data, length, flags);
}

ssize_t io_read(int fd, void* data, size_t length,
                int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::read(fd, data, length);
}

ssize_t io_pread(int fd, void* data, size_t length, off_t offset,
                 int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::pread(fd, data, length, offset);
}

ssize_t io_receive_exact(int fd, void* data, size_t length, int flags,
                         int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  size_t offset = 0;
  while (offset != length) {
    const ssize_t result = ::recv(
        fd, static_cast<u_char*>(data) + offset, length - offset, flags);
    if (result > 0) {
      offset += size_t(result);
      continue;
    }
    if (result == 0) {
      break;
    }
    if (errno != EINTR) {
      return -1;
    }
  }
  return ssize_t(offset);
}

ssize_t io_receive_exact_then(
    int fd, void* data, size_t length, int flags,
    IoExecutor::ReceiveProcessor processor, void* context,
    int timeout_ms) noexcept {
  if (processor == nullptr) {
    errno = EINVAL;
    return -1;
  }
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  const ssize_t received = io_receive_exact(
      fd, data, length, flags, timeout_ms);
  if (received != ssize_t(length)) {
    return received;
  }
  const int processed = processor(context, length);
  if (processed > 0) {
    return received;
  }
  errno = processed < 0 ? -processed : EIO;
  return -1;
}

ssize_t io_receive_until(int fd, void* data, size_t length, int flags,
                         IoExecutor::ReceiveProcessor processor,
                         void* context, int) noexcept {
  if (processor == nullptr || length == 0) {
    errno = EINVAL;
    return -1;
  }
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  size_t total = 0;
  for (;;) {
    const ssize_t result = ::recv(fd, data, length, flags);
    if (result > 0) {
      total += size_t(result);
      const int processed = processor(context, size_t(result));
      if (processed > 0) {
        return ssize_t(total);
      }
      if (processed < 0) {
        errno = -processed;
        return -1;
      }
      continue;
    }
    if (result == 0) {
      return ssize_t(total);
    }
    if (errno != EINTR) {
      return -1;
    }
  }
}

ssize_t io_send(int fd, const void* data, size_t length, int flags,
                int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::send(fd, data, length, flags);
}

ssize_t io_send_exact(int fd, const void* data, size_t length, int flags,
                      int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  size_t offset = 0;
  while (offset != length) {
    const ssize_t result = ::send(
        fd, static_cast<const u_char*>(data) + offset,
        length - offset, flags);
    if (result > 0) {
      offset += size_t(result);
      continue;
    }
    if (result == 0) {
      errno = EIO;
      return -1;
    }
    if (errno != EINTR) {
      return -1;
    }
  }
  return ssize_t(offset);
}

ssize_t io_pwrite(int fd, const void* data, size_t length,
                  off_t offset, int,
                  bool) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::pwrite(fd, data, length, offset);
}

ssize_t io_splice(int input_fd, off_t* input_offset,
                  int output_fd, off_t* output_offset,
                  size_t length, unsigned flags,
                  int, bool) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::splice(input_fd, input_offset, output_fd, output_offset,
                  length, flags);
}

ssize_t io_splice_exact(int input_fd, off_t* input_offset,
                        int output_fd, off_t* output_offset,
                        size_t length, unsigned flags,
                        int, size_t* calls,
                        bool) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  size_t transferred = 0;
  while (transferred != length) {
    if (calls != nullptr) {
      ++*calls;
    }
    const ssize_t result = ::splice(
        input_fd, input_offset, output_fd, output_offset,
        length - transferred, flags);
    if (result > 0) {
      transferred += size_t(result);
      continue;
    }
    if (result == 0) {
      break;
    }
    if (errno != EINTR) {
      return -1;
    }
  }
  return ssize_t(transferred);
}

int io_connect(int fd, const sockaddr* address,
               socklen_t address_length, int) noexcept {
  if (current_io_executor != nullptr) {
    // A legacy blocking operation on the reactor is an implementation bug,
    // never a reason to recreate the removed synchronous handoff bridge.
    errno = EDEADLK;
    return -1;
  }
  return ::connect(fd, address, address_length);
}

// Pipe and descriptor data movement.
[[noreturn]] void pipe_throw_errno(const char* operation) {
  throw std::system_error(errno, std::generic_category(), operation);
}

Pipe::Pipe(UniqueFd read_end, UniqueFd write_end,
           size_t capacity) noexcept
    : read_end_(std::move(read_end)),
      write_end_(std::move(write_end)),
      capacity_(capacity) {}

Pipe Pipe::create(size_t preferred_capacity) {
  int raw_fds[2] = {-1, -1};
  if (::pipe2(raw_fds, O_CLOEXEC | O_NONBLOCK) != 0) {
    pipe_throw_errno("pipe2");
  }

  UniqueFd read_end(raw_fds[0]);
  UniqueFd write_end(raw_fds[1]);

  int capacity = ::fcntl(write_end.get(), F_GETPIPE_SZ);
  if (capacity < 0) {
    pipe_throw_errno("fcntl(F_GETPIPE_SZ)");
  }

  if (preferred_capacity > static_cast<size_t>(capacity)) {
    const size_t bounded = std::min(
        preferred_capacity,
        static_cast<size_t>(std::numeric_limits<int>::max()));
    const int requested = static_cast<int>(bounded);
    const int resized = ::fcntl(write_end.get(), F_SETPIPE_SZ, requested);
    if (resized >= 0) {
      capacity = resized;
    } else if (errno != EPERM && errno != EINVAL) {
      pipe_throw_errno("fcntl(F_SETPIPE_SZ)");
    }
  }

  return Pipe(std::move(read_end), std::move(write_end),
              static_cast<size_t>(capacity));
}

size_t splice_exact(int source_fd, int destination_fd,
                    size_t length, unsigned int flags, size_t* calls) {
  const ssize_t result = io_splice_exact(
      source_fd, nullptr, destination_fd, nullptr, length, flags, 0, calls,
      false);
  if (result < 0) {
    pipe_throw_errno("splice");
  }
  if (size_t(result) != length) {
    throw std::system_error(
        std::make_error_code(std::errc::connection_reset),
        "splice reached EOF before the requested length");
  }
  return length;
}

size_t splice_from_fd_exact(int source_fd, uint64_t& source_offset,
                            int destination_fd, size_t length,
                            unsigned int flags) {
  size_t transferred = 0;
  while (transferred < length) {
    off_t offset = off_t(source_offset);
    const ssize_t result = io_splice(
        source_fd, &offset, destination_fd, nullptr,
        length - transferred, flags);
    if (result > 0) {
      transferred += size_t(result);
      source_offset = uint64_t(offset);
      continue;
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::connection_reset),
          "splice reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    pipe_throw_errno("splice(from fd)");
  }
  return transferred;
}

size_t splice_some(int source_fd, uint64_t* source_offset,
                   int destination_fd, size_t length,
                   unsigned int flags) {
  for (;;) {
    off_t offset = source_offset == nullptr ? 0 : off_t(*source_offset);
    off_t* position = source_offset == nullptr ? nullptr : &offset;
    const ssize_t result = io_splice(source_fd, position, destination_fd,
                                     nullptr, length, flags);
    if (result > 0) {
      if (source_offset != nullptr) {
        *source_offset = uint64_t(offset);
      }
      return size_t(result);
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::connection_reset),
          "splice reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return 0;
    }
    pipe_throw_errno("splice");
  }
}

size_t tee_exact(int source_fd, int destination_fd,
                 size_t length, unsigned int flags) {
  for (;;) {
    const ssize_t result = ::tee(source_fd, destination_fd, length, flags);
    if (result == ssize_t(length)) {
      return length;
    }
    if (result > 0) {
      throw std::system_error(
          std::make_error_code(std::errc::io_error),
          "tee returned a short clone");
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::connection_reset),
          "tee reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    pipe_throw_errno("tee");
  }
}

size_t splice_to_fd_exact(int source_fd, int destination_fd,
                          uint64_t& destination_offset,
                          size_t length, unsigned int flags) {
  size_t transferred = 0;
  while (transferred < length) {
    off_t offset = static_cast<off_t>(destination_offset);
    const ssize_t result =
        io_splice(source_fd, nullptr, destination_fd, &offset,
                  length - transferred, flags, 0, true);
    if (result > 0) {
      transferred += static_cast<size_t>(result);
      destination_offset = static_cast<uint64_t>(offset);
      continue;
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::connection_reset),
          "splice reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    pipe_throw_errno("splice(to fd)");
  }
  return transferred;
}

size_t sendfile_exact(int socket_fd, int source_fd,
                      uint64_t& source_offset, size_t length) {
  size_t transferred = 0;
  while (transferred < length) {
    off_t offset = static_cast<off_t>(source_offset);
    const ssize_t result = ::sendfile(socket_fd, source_fd, &offset,
                                      length - transferred);
    if (result > 0) {
      transferred += static_cast<size_t>(result);
      source_offset = static_cast<uint64_t>(offset);
      continue;
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::io_error),
          "sendfile reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    pipe_throw_errno("sendfile");
  }
  return transferred;
}

void write_all(int fd, std::span<const std::byte> bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t result = ::write(fd, bytes.data() + offset,
                                   bytes.size() - offset);
    if (result > 0) {
      offset += static_cast<size_t>(result);
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    pipe_throw_errno("write");
  }
}

void send_all(int socket_fd, std::span<const std::byte> bytes) {
  const ssize_t result = io_send_exact(
      socket_fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
  if (result < 0) {
    pipe_throw_errno("send");
  }
  if (size_t(result) != bytes.size()) {
    throw std::system_error(
        std::make_error_code(std::errc::io_error),
        "send reached EOF before the requested length");
  }
}

void receive_all(int socket_fd, std::span<std::byte> bytes) {
  const ssize_t result = io_receive_exact(
      socket_fd, bytes.data(), bytes.size());
  if (result < 0) {
    pipe_throw_errno("recv");
  }
  if (size_t(result) != bytes.size()) {
    throw std::system_error(
        std::make_error_code(std::errc::connection_reset),
        "recv reached EOF before the requested length");
  }
}

void read_all(int fd, std::span<std::byte> bytes) {
  size_t offset = 0;
  while (offset < bytes.size()) {
    const ssize_t result = ::read(fd, bytes.data() + offset,
                                  bytes.size() - offset);
    if (result > 0) {
      offset += static_cast<size_t>(result);
      continue;
    }
    if (result == 0) {
      throw std::system_error(
          std::make_error_code(std::errc::connection_reset),
          "read reached EOF before the requested length");
    }
    if (errno == EINTR) {
      continue;
    }
    pipe_throw_errno("read");
  }
}

// TCP connection and timeout handling.
using Clock = std::chrono::steady_clock;

int remaining_ms(Clock::time_point deadline) {
  const auto remaining =
      std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                            Clock::now());
  if (remaining.count() <= 0) {
    return 0;
  }
  return static_cast<int>(
      std::min<int64_t>(remaining.count(), INT_MAX));
}

short wait_for_events(int fd, short events, int timeout_ms) {
  const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
  for (;;) {
    pollfd descriptor{
        .fd = fd,
        .events = events,
        .revents = 0,
    };
    const int result = ::poll(&descriptor, 1, remaining_ms(deadline));
    if (result > 0) {
      return descriptor.revents;
    }
    if (result == 0) {
      return 0;
    }
    if (errno != EINTR) {
      throw std::system_error(errno, std::generic_category(), "poll");
    }
    if (remaining_ms(deadline) == 0) {
      return 0;
    }
  }
}

void configure_blocking_socket(int fd, int io_timeout_ms) {
  const int flags = ::fcntl(fd, F_GETFL);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "fcntl(blocking socket)");
  }

  set_socket_receive_timeout(fd, io_timeout_ms);
  timeval timeout{
      .tv_sec = io_timeout_ms / 1'000,
      .tv_usec = (io_timeout_ms % 1'000) * 1'000,
  };
  if (::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                   sizeof(timeout)) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "setsockopt(socket timeout)");
  }
  const int one = 1;
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "setsockopt(TCP_NODELAY)");
  }
}

size_t socket_receive_buffer_capacity(int fd) noexcept {
  int actual = 0;
  socklen_t size = sizeof(actual);
  if (::getsockopt(fd, SOL_SOCKET, SO_RCVBUF,
                   &actual, &size) != 0 || size != sizeof(actual)) {
    return 0;
  }
  // Linux reports twice the user-visible receive-buffer allowance.
  return actual > 0 ? size_t(actual) / 2 : 0;
}

bool configure_socket_receive_buffer(int fd, size_t requested) noexcept {
  if (requested == 0) {
    return true;
  }

  static std::atomic_flag warned = ATOMIC_FLAG_INIT;
  const auto warn = [&](const char* reason, size_t effective) {
    if (!warned.test_and_set(std::memory_order_relaxed)) {
      fprintf(stderr,
              "warning: unable to obtain the requested %zu-byte TCP "
              "receive buffer (%s, effective %zu bytes); reads will "
              "fall back to TCP receive autotuning; consider raising "
              "net.core.rmem_max\n",
              requested, reason, effective);
    }
  };

  if (requested > size_t(INT_MAX)) {
    warn("value exceeds INT_MAX", 0);
    return false;
  }
  const int value = int(requested);
  if (::setsockopt(fd, SOL_SOCKET, SO_RCVBUF,
                   &value, sizeof(value)) != 0) {
    const int error = errno;
    warn(strerror(error), socket_receive_buffer_capacity(fd));
    return false;
  }

  const size_t effective = socket_receive_buffer_capacity(fd);
  if (effective == 0) {
    warn("SO_RCVBUF verification failed", 0);
    return false;
  }
  if (effective < requested) {
    warn("the kernel capped SO_RCVBUF", effective);
    return false;
  }
  return true;
}

UniqueFd connect_tcp(std::string_view host, uint16_t port,
                     int connect_timeout_ms, int io_timeout_ms,
                     size_t receive_buffer_size) {
  if (connect_timeout_ms <= 0 || io_timeout_ms <= 0) {
    throw std::invalid_argument("socket timeouts must be positive");
  }

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  const std::string host_string(host);
  const std::string service = std::to_string(port);
  addrinfo* raw_addresses = nullptr;
  const int lookup = ::getaddrinfo(host_string.c_str(), service.c_str(),
                                   &hints, &raw_addresses);
  if (lookup != 0) {
    throw std::runtime_error(std::string("getaddrinfo: ") +
                             gai_strerror(lookup));
  }
  std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> addresses(
      raw_addresses, ::freeaddrinfo);

  int last_error = ECONNREFUSED;
  for (addrinfo* address = addresses.get(); address != nullptr;
       address = address->ai_next) {
    UniqueFd socket(::socket(address->ai_family,
                             address->ai_socktype | SOCK_CLOEXEC |
                                 SOCK_NONBLOCK,
                             address->ai_protocol));
    if (!socket) {
      last_error = errno;
      continue;
    }
    if (!configure_socket_receive_buffer(
            socket.get(), receive_buffer_size)) {
      socket.reset(::socket(address->ai_family,
                            address->ai_socktype | SOCK_CLOEXEC |
                                SOCK_NONBLOCK,
                            address->ai_protocol));
      if (!socket) {
        last_error = errno;
        continue;
      }
    }

    if (io_connect(socket.get(), address->ai_addr, address->ai_addrlen,
                   connect_timeout_ms) != 0) {
      if (errno != EINPROGRESS) {
        last_error = errno;
        continue;
      }
      const short events = wait_for_events(socket.get(), POLLOUT,
                                           connect_timeout_ms);
      if (events == 0) {
        last_error = ETIMEDOUT;
        continue;
      }
      int socket_error = 0;
      socklen_t error_size = sizeof(socket_error);
      if (::getsockopt(socket.get(), SOL_SOCKET, SO_ERROR, &socket_error,
                       &error_size) != 0) {
        last_error = errno;
        continue;
      }
      if (socket_error != 0) {
        last_error = socket_error;
        continue;
      }
    }

    configure_blocking_socket(socket.get(), io_timeout_ms);
    return socket;
  }

  throw std::system_error(last_error, std::generic_category(), "connect");
}

void set_socket_receive_timeout(int fd, int timeout_ms) {
  if (fd < 0 || timeout_ms <= 0) {
    throw std::invalid_argument("invalid receive timeout argument");
  }
  timeval timeout{
      .tv_sec = timeout_ms / 1'000,
      .tv_usec = (timeout_ms % 1'000) * 1'000,
  };
  if (::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout)) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "setsockopt(SO_RCVTIMEO)");
  }
}
