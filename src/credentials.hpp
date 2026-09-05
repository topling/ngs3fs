#pragma once

#include "s3.hpp"

#include <atomic>
#include <condition_variable>
#include <exception>
#include <errno.h>
#include <memory>
#include <mutex>
#include <stdint.h>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

struct CredentialProviderOptions {
  std::string region;
  int connect_timeout_ms        = kConnectTimeoutMs;
  int request_timeout_ms        = kRequestIoTimeoutMs;
  int protocol_probe_timeout_ms = kProtocolProbeTimeoutMs;
  int metadata_timeout_ms       = 1000;
  int shared_reload_ms          = 60 * 1000;
};

struct CredentialRefreshPending : std::system_error {
  CredentialRefreshPending()
      : std::system_error(EAGAIN, std::generic_category(),
                          "credentials are refreshing; retry later") {}
};

class CredentialProvider {
 public:
  explicit CredentialProvider(CredentialProviderOptions options);
  ~CredentialProvider();

  CredentialProvider(const CredentialProvider&) = delete;
  CredentialProvider& operator=(const CredentialProvider&) = delete;

  const Credentials& get();
  const char* source_name() const noexcept;
  bool refreshable() const noexcept;

 public:
  enum Source {
    SOURCE_ANONYMOUS,
    SOURCE_ENVIRONMENT,
    SOURCE_SHARED_FILE,
    SOURCE_PROCESS,
    SOURCE_WEB_IDENTITY,
    SOURCE_CONTAINER,
    SOURCE_IMDS,
  };

  struct Snapshot {
    Credentials credentials;
    int64_t expiration = 0;
  };

  struct Profile {
    Credentials credentials;
    std::string process;
    std::string token_file;
    std::string role_arn;
    std::string role_session_name;
    std::string credential_source;
    std::string source_profile;
    std::string sso_session;
  };

 private:
  friend struct CredentialProviderTestAccess;
  void discover();
  void refresh_locked(uint64_t now_ns);
  void refresh_loop(std::stop_token stop) noexcept;
  Snapshot load_source();
  Snapshot load_process() const;
  Snapshot load_web_identity() const;
  Snapshot load_container() const;
  Snapshot load_imds() const;
  Snapshot load_shared_file() const;
  void install(Snapshot value, uint64_t now_ns);
  void schedule_refresh(const Snapshot& value, uint64_t now_ns);
  Profile load_profile() const;

  CredentialProviderOptions options_;
  Source source_ = SOURCE_ANONYMOUS;
  std::string profile_;
  std::string shared_credentials_file_;
  std::string config_file_;
  std::string process_;
  std::string token_file_;
  std::string role_arn_;
  std::string role_session_name_;
  std::string container_uri_;
  std::string container_token_;
  std::string imds_endpoint_;
  std::vector<std::unique_ptr<Snapshot>> snapshots_;
  std::atomic<Snapshot*> current_{nullptr};
  std::atomic<uint64_t> next_refresh_ns_{UINT64_MAX};
  std::mutex mutex_;
  std::condition_variable condition_;
  std::exception_ptr refresh_error_;
  std::jthread refresher_;
};
