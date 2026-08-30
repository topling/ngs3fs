#define _FILE_OFFSET_BITS 64

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

typedef struct {
  const char* directory;
  size_t      files;
  size_t      threads;
  size_t      operations;
  size_t      file_size;
  size_t      maximum_read;
  size_t      page_size;
  uint64_t    seed;
} Config;

typedef struct {
  const Config* config;
  pthread_mutex_t mutex;
  pthread_cond_t  condition;
  bool            start;
  atomic_int      failed;
} Shared;

typedef struct {
  Shared* shared;
  size_t  id;
} Worker;

static void usage(const char* program) {
  fprintf(stderr,
          "usage: %s -d DIR [-f FILES] [-t THREADS] [-n OPERATIONS] "
          "[-s FILE_SIZE] [-r MAX_READ] [-S SEED]\n",
          program);
}

static bool parse_size(const char* text, size_t* value) {
  char* end = NULL;
  errno = 0;
  const unsigned long long parsed = strtoull(text, &end, 0);
  if (errno != 0 || end == text || *end != '\0' || parsed > SIZE_MAX) {
    return false;
  }
  *value = (size_t)parsed;
  return true;
}

static bool parse_u64(const char* text, uint64_t* value) {
  char* end = NULL;
  errno = 0;
  const unsigned long long parsed = strtoull(text, &end, 0);
  if (errno != 0 || end == text || *end != '\0') {
    return false;
  }
  *value = (uint64_t)parsed;
  return true;
}

static u_char expected_byte(size_t file, uint64_t offset) {
  return (u_char)(file * 37 + offset + (offset >> 8) + (offset >> 16));
}

static bool file_path(char path[PATH_MAX], const Config* config,
                      size_t file) {
  const int length = snprintf(path, PATH_MAX, "%s/file-%04zu.bin",
                              config->directory, file);
  return length > 0 && length < PATH_MAX;
}

static bool write_all(int fd, const u_char* data, size_t size) {
  size_t offset = 0;
  while (offset != size) {
    const ssize_t written = write(fd, data + offset, size - offset);
    if (written > 0) {
      offset += (size_t)written;
      continue;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    return false;
  }
  return true;
}

static bool create_file(const Config* config, size_t file, u_char* buffer,
                        size_t buffer_size) {
  char path[PATH_MAX];
  if (!file_path(path, config, file)) {
    fprintf(stderr, "random-read path is too long\n");
    return false;
  }
  const int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    fprintf(stderr, "open for write failed: %s: %s\n", path,
            strerror(errno));
    return false;
  }

  bool ok = true;
  size_t offset = 0;
  while (offset != config->file_size) {
    const size_t size = config->file_size - offset < buffer_size
                            ? config->file_size - offset
                            : buffer_size;
    for (size_t i = 0; i != size; ++i) {
      buffer[i] = expected_byte(file, offset + i);
    }
    if (!write_all(fd, buffer, size)) {
      fprintf(stderr, "sequential write failed: %s: %s\n", path,
              strerror(errno));
      ok = false;
      break;
    }
    offset += size;
  }
  if (close(fd) != 0) {
    fprintf(stderr, "close after write failed: %s: %s\n", path,
            strerror(errno));
    ok = false;
  }
  return ok;
}

static uint64_t random_u64(uint64_t* state) {
  uint64_t value = *state;
  value ^= value << 13;
  value ^= value >> 7;
  value ^= value << 17;
  *state = value;
  return value;
}

static void report_failure(Shared* shared, const char* operation,
                           size_t worker, size_t file, uint64_t offset,
                           size_t size) {
  if (atomic_exchange_explicit(&shared->failed, 1,
                               memory_order_relaxed) == 0) {
    fprintf(stderr,
            "random-read failure: operation=%s worker=%zu file=%zu "
            "offset=%llu size=%zu errno=%d (%s)\n",
            operation, worker, file, (unsigned long long)offset, size,
            errno, strerror(errno));
  }
}

static void* read_worker(void* argument) {
  Worker* worker = argument;
  Shared* shared = worker->shared;
  const Config* config = shared->config;

  pthread_mutex_lock(&shared->mutex);
  while (!shared->start) {
    pthread_cond_wait(&shared->condition, &shared->mutex);
  }
  pthread_mutex_unlock(&shared->mutex);

  u_char* buffer = malloc(config->maximum_read);
  if (buffer == NULL) {
    report_failure(shared, "malloc", worker->id, 0, 0,
                   config->maximum_read);
    return NULL;
  }

  uint64_t random = config->seed ^
                    ((worker->id + 1) * UINT64_C(0x9e3779b97f4a7c15));
  if (random == 0) {
    random = 1;
  }
  for (size_t operation = 0;
       operation != config->operations &&
       atomic_load_explicit(&shared->failed, memory_order_relaxed) == 0;
       ++operation) {
    const size_t file = (size_t)(random_u64(&random) % config->files);
    size_t size = 1 + (size_t)(random_u64(&random) % config->maximum_read);
    if (size > config->file_size) {
      size = config->file_size;
    }
    const uint64_t limit = config->file_size - size + 1;
    const uint64_t offset = random_u64(&random) % limit;
    char path[PATH_MAX];
    if (!file_path(path, config, file)) {
      errno = ENAMETOOLONG;
      report_failure(shared, "path", worker->id, file, offset, size);
      break;
    }

    const int fd = open(path, O_RDONLY);
    if (fd < 0) {
      report_failure(shared, "open", worker->id, file, offset, size);
      break;
    }
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
    if (((operation + worker->id) & 1) == 0) {
      size_t done = 0;
      while (done != size) {
        const ssize_t count = pread(fd, buffer + done, size - done,
                                    (off_t)(offset + done));
        if (count > 0) {
          done += (size_t)count;
          continue;
        }
        if (count < 0 && errno == EINTR) {
          continue;
        }
        if (count == 0) {
          errno = EIO;
        }
        report_failure(shared, "pread", worker->id, file, offset, size);
        break;
      }
      if (done == size) {
        for (size_t i = 0; i != size; ++i) {
          if (buffer[i] != expected_byte(file, offset + i)) {
            errno = EILSEQ;
            report_failure(shared, "pread-verify", worker->id, file,
                           offset + i, size);
            break;
          }
        }
      }
    } else {
      const uint64_t map_offset = offset - offset % config->page_size;
      const size_t displacement = (size_t)(offset - map_offset);
      const size_t map_size = displacement + size;
      void* mapping = mmap(NULL, map_size, PROT_READ, MAP_SHARED, fd,
                           (off_t)map_offset);
      if (mapping == MAP_FAILED) {
        report_failure(shared, "mmap", worker->id, file, offset, size);
      } else {
        (void)madvise(mapping, map_size, MADV_RANDOM);
        const u_char* data = (const u_char*)mapping + displacement;
        for (size_t i = 0; i != size; ++i) {
          if (data[i] != expected_byte(file, offset + i)) {
            errno = EILSEQ;
            report_failure(shared, "mmap-verify", worker->id, file,
                           offset + i, size);
            break;
          }
        }
        if (munmap(mapping, map_size) != 0) {
          report_failure(shared, "munmap", worker->id, file, offset, size);
        }
      }
    }
    (void)posix_fadvise(fd, (off_t)offset, (off_t)size,
                       POSIX_FADV_DONTNEED);
    if (close(fd) != 0) {
      report_failure(shared, "close", worker->id, file, offset, size);
      break;
    }
  }

  free(buffer);
  return NULL;
}

static bool remove_files(const Config* config) {
  bool ok = true;
  for (size_t file = 0; file != config->files; ++file) {
    char path[PATH_MAX];
    if (!file_path(path, config, file) || unlink(path) != 0) {
      fprintf(stderr, "unlink failed: file=%zu: %s\n", file,
              strerror(errno));
      ok = false;
    }
  }
  if (rmdir(config->directory) != 0) {
    fprintf(stderr, "rmdir failed: %s: %s\n", config->directory,
            strerror(errno));
    ok = false;
  }
  return ok;
}

int main(int argc, char** argv) {
  Config config = {
      .directory    = NULL,
      .files        = 32,
      .threads      = 16,
      .operations   = 128,
      .file_size    = 4 * 1024 * 1024,
      .maximum_read = 256 * 1024,
      .page_size    = 0,
      .seed          = UINT64_C(0x4e47533346535244),
  };
  static const struct option options[] = {
      {"directory", required_argument, NULL, 'd'},
      {"files", required_argument, NULL, 'f'},
      {"threads", required_argument, NULL, 't'},
      {"operations", required_argument, NULL, 'n'},
      {"file-size", required_argument, NULL, 's'},
      {"maximum-read", required_argument, NULL, 'r'},
      {"seed", required_argument, NULL, 'S'},
      {"help", no_argument, NULL, 'h'},
      {NULL, 0, NULL, 0},
  };

  int option;
  while ((option = getopt_long(argc, argv, "d:f:t:n:s:r:S:h", options,
                               NULL)) != -1) {
    bool valid = true;
    switch (option) {
      case 'd': config.directory = optarg; break;
      case 'f': valid = parse_size(optarg, &config.files); break;
      case 't': valid = parse_size(optarg, &config.threads); break;
      case 'n': valid = parse_size(optarg, &config.operations); break;
      case 's': valid = parse_size(optarg, &config.file_size); break;
      case 'r': valid = parse_size(optarg, &config.maximum_read); break;
      case 'S': valid = parse_u64(optarg, &config.seed); break;
      case 'h': usage(argv[0]); return 0;
      default: usage(argv[0]); return 2;
    }
    if (!valid) {
      fprintf(stderr, "invalid numeric option: %s\n", optarg);
      return 2;
    }
  }
  if (config.directory == NULL || optind != argc || config.files < 2 ||
      config.threads == 0 || config.operations == 0 ||
      config.file_size == 0 || config.maximum_read == 0) {
    usage(argv[0]);
    return 2;
  }
  const long page_size = sysconf(_SC_PAGESIZE);
  if (page_size <= 0) {
    fprintf(stderr, "unable to determine page size\n");
    return 1;
  }
  config.page_size = (size_t)page_size;
  if (mkdir(config.directory, 0755) != 0) {
    fprintf(stderr, "mkdir failed: %s: %s\n", config.directory,
            strerror(errno));
    return 1;
  }

  const size_t prepare_size = config.maximum_read < 256 * 1024
                                  ? config.maximum_read
                                  : 256 * 1024;
  u_char* prepare_buffer = malloc(prepare_size);
  if (prepare_buffer == NULL) {
    fprintf(stderr, "prepare buffer allocation failed\n");
    return 1;
  }
  for (size_t file = 0; file != config.files; ++file) {
    if (!create_file(&config, file, prepare_buffer, prepare_size)) {
      free(prepare_buffer);
      return 1;
    }
  }
  free(prepare_buffer);

  Shared shared = {
      .config    = &config,
      .mutex     = PTHREAD_MUTEX_INITIALIZER,
      .condition = PTHREAD_COND_INITIALIZER,
      .start     = false,
      .failed    = 0,
  };
  pthread_t* threads = calloc(config.threads, sizeof(*threads));
  Worker* workers = calloc(config.threads, sizeof(*workers));
  if (threads == NULL || workers == NULL) {
    fprintf(stderr, "worker allocation failed\n");
    free(threads);
    free(workers);
    return 1;
  }

  size_t created = 0;
  for (; created != config.threads; ++created) {
    workers[created].shared = &shared;
    workers[created].id     = created;
    const int error = pthread_create(&threads[created], NULL, read_worker,
                                     &workers[created]);
    if (error != 0) {
      errno = error;
      fprintf(stderr, "pthread_create failed: %s\n", strerror(errno));
      atomic_store_explicit(&shared.failed, 1, memory_order_relaxed);
      break;
    }
  }
  pthread_mutex_lock(&shared.mutex);
  shared.start = true;
  pthread_cond_broadcast(&shared.condition);
  pthread_mutex_unlock(&shared.mutex);

  for (size_t i = 0; i != created; ++i) {
    const int error = pthread_join(threads[i], NULL);
    if (error != 0) {
      errno = error;
      fprintf(stderr, "pthread_join failed: %s\n", strerror(errno));
      atomic_store_explicit(&shared.failed, 1, memory_order_relaxed);
    }
  }
  free(threads);
  free(workers);

  const bool passed = atomic_load_explicit(&shared.failed,
                                            memory_order_relaxed) == 0;
  if (passed && !remove_files(&config)) {
    return 1;
  }
  if (!passed) {
    return 1;
  }
  printf("random-read stress passed: access=pread,mmap files=%zu threads=%zu "
         "operations=%zu file_size=%zu maximum_read=%zu seed=%llu\n",
         config.files, config.threads, config.operations, config.file_size,
         config.maximum_read, (unsigned long long)config.seed);
  return 0;
}
