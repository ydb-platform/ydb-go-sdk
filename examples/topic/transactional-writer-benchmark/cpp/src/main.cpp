#include <ydb-cpp-sdk/client/helpers/helpers.h>
#include <ydb-cpp-sdk/client/params/params.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/topic/client.h>
#include <ydb-cpp-sdk/client/topic/producer.h>
#include <ydb-cpp-sdk/client/topic/write_events.h>
#include <ydb-cpp-sdk/client/topic/write_session.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#ifndef YDB_CPP_SDK_VERSION
#define YDB_CPP_SDK_VERSION "unknown"
#endif

namespace {

using Clock = std::chrono::steady_clock;
using Milliseconds = std::chrono::milliseconds;
using Nanoseconds = std::chrono::nanoseconds;
using NYdb::EStatus;
using NYdb::TStatus;
using NYdb::NQuery::TQueryClient;
using NYdb::NTopic::IWriteSession;
using NYdb::NTopic::TContinuationToken;
using NYdb::NTopic::TTopicClient;
using NYdb::NTopic::TWriteMessage;
using WriteEvent = NYdb::NTopic::TWriteSessionEvent;

constexpr std::uint32_t kafka_hash_seed = 0x9747b28cU;
constexpr std::uint32_t kafka_hash_mask = 0x7fffffffU;

std::atomic<bool> interrupted = false;

void handle_signal(int) { interrupted.store(true); }

std::string status_error(std::string operation, const TStatus& status) {
    operation += " failed with YDB status ";
    operation += std::to_string(static_cast<std::size_t>(status.GetStatus()));
    const auto issues = status.GetIssues().ToString(true);
    if (!issues.empty()) {
        operation += ": ";
        operation.append(issues.data(), issues.size());
    }
    return operation;
}

TStatus copy_status(const TStatus& status) {
    return TStatus(status.GetStatus(), NYdb::NIssue::TIssues(status.GetIssues()));
}

TStatus local_error(EStatus status, const std::string& message) {
    return TStatus(status,
                   NYdb::NIssue::TIssues{NYdb::NIssue::TIssue(message)});
}

Milliseconds parse_duration(std::string_view value) {
    const std::pair<std::string_view, double> suffixes[] = {
        {"ms", 1.0},
        {"s", 1000.0},
        {"m", 60'000.0},
        {"h", 3'600'000.0},
    };
    for (const auto& [suffix, multiplier] : suffixes) {
        if (value.size() <= suffix.size() ||
            value.substr(value.size() - suffix.size()) != suffix) {
            continue;
        }
        const auto number = std::stod(
            std::string(value.substr(0, value.size() - suffix.size())));
        if (!std::isfinite(number) || number < 0.0) {
            break;
        }
        return Milliseconds(static_cast<std::int64_t>(number * multiplier));
    }
    throw std::runtime_error("invalid duration: " + std::string(value));
}

std::string duration_string(Milliseconds duration) {
    if (duration == Milliseconds::zero()) {
        return "0s";
    }
    if (duration.count() % 60'000 == 0) {
        return std::to_string(duration.count() / 60'000) + "m0s";
    }
    if (duration.count() % 1000 == 0) {
        return std::to_string(duration.count() / 1000) + "s";
    }
    return std::to_string(duration.count()) + "ms";
}

std::string generated_at() {
    const auto now = std::chrono::system_clock::now();
    const auto value = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &value);
#else
    gmtime_r(&value, &tm);
#endif
    std::ostringstream output;
    output << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return output.str();
}

std::string default_run_id() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return "cpp-" +
           std::to_string(
               std::chrono::duration_cast<std::chrono::nanoseconds>(now)
                   .count());
}

enum class WriterMode { Single, Many };
enum class RoutingMode { Key, BoundedKey, PartitionId };

std::string to_string(WriterMode value) {
    return value == WriterMode::Single ? "single" : "many";
}

std::string to_string(RoutingMode value) {
    switch (value) {
        case RoutingMode::Key:
            return "key";
        case RoutingMode::BoundedKey:
            return "bounded-key";
        case RoutingMode::PartitionId:
            return "partition-id";
    }
    throw std::logic_error("unknown routing mode");
}

struct Config {
    std::string dsn;
    std::string topic_path;
    std::string table_path;
    std::string run_id = default_run_id();
    std::string label;
    std::string producer_id_prefix = run_id;
    WriterMode mode = WriterMode::Many;
    RoutingMode routing = RoutingMode::Key;
    bool auto_seq_no = true;
    bool query_retries = true;
    Milliseconds duration{5000};
    Milliseconds warmup{2000};
    Milliseconds transaction_timeout{30'000};
    std::size_t concurrency = 4;
    std::size_t messages_per_transaction = 1;
    std::size_t message_size_bytes = 1024;
    std::size_t latency_sample_every = 1;
    std::size_t max_errors = 10;
    bool auto_split = false;
    Milliseconds auto_split_poll_interval{250};
    bool anonymous = false;
    bool skip_table_write = false;
};

bool parse_bool(std::string_view value) {
    if (value == "true" || value == "1") {
        return true;
    }
    if (value == "false" || value == "0") {
        return false;
    }
    throw std::runtime_error("invalid boolean: " + std::string(value));
}

std::size_t parse_size(std::string_view name, std::string_view value) {
    std::size_t consumed = 0;
    const auto parsed = std::stoull(std::string(value), &consumed);
    if (consumed != value.size() ||
        parsed > std::numeric_limits<std::size_t>::max()) {
        throw std::runtime_error("invalid " + std::string(name) + ": " +
                                 std::string(value));
    }
    return static_cast<std::size_t>(parsed);
}

std::pair<std::string, std::optional<std::string>> split_argument(
    std::string argument) {
    const auto separator = argument.find('=');
    if (separator == std::string::npos) {
        return {std::move(argument), std::nullopt};
    }
    auto value = argument.substr(separator + 1);
    argument.resize(separator);
    return {std::move(argument), std::move(value)};
}

Config parse_config(int argc, char* argv[]) {
    Config config;
    const auto value_for = [&](int& index,
                               const std::string& name,
                               std::optional<std::string> inline_value) {
        if (inline_value) {
            return *inline_value;
        }
        if (index + 1 >= argc) {
            throw std::runtime_error(name + " requires a value");
        }
        return std::string(argv[++index]);
    };

    for (int index = 1; index < argc; ++index) {
        auto [name, inline_value] = split_argument(argv[index]);
        const auto value = [&] {
            return value_for(index, name, std::move(inline_value));
        };
        if (name == "--dsn") {
            config.dsn = value();
        } else if (name == "--topic") {
            config.topic_path = value();
        } else if (name == "--table") {
            config.table_path = value();
        } else if (name == "--run-id") {
            config.run_id = value();
        } else if (name == "--label") {
            config.label = value();
        } else if (name == "--producer-id-prefix") {
            config.producer_id_prefix = value();
        } else if (name == "--mode") {
            const auto parsed = value();
            if (parsed == "single") {
                config.mode = WriterMode::Single;
            } else if (parsed == "many") {
                config.mode = WriterMode::Many;
            } else {
                throw std::runtime_error("--mode must be single or many");
            }
        } else if (name == "--routing") {
            const auto parsed = value();
            if (parsed == "key") {
                config.routing = RoutingMode::Key;
            } else if (parsed == "bounded-key") {
                config.routing = RoutingMode::BoundedKey;
            } else if (parsed == "partition-id") {
                config.routing = RoutingMode::PartitionId;
            } else {
                throw std::runtime_error(
                    "--routing must be key, bounded-key, or partition-id");
            }
        } else if (name == "--auto-seq-no") {
            config.auto_seq_no = parse_bool(value());
        } else if (name == "--query-retries") {
            config.query_retries = parse_bool(value());
        } else if (name == "--duration") {
            config.duration = parse_duration(value());
        } else if (name == "--warmup") {
            config.warmup = parse_duration(value());
        } else if (name == "--transaction-timeout") {
            config.transaction_timeout = parse_duration(value());
        } else if (name == "--concurrency") {
            config.concurrency = parse_size(name, value());
        } else if (name == "--messages-per-tx") {
            config.messages_per_transaction = parse_size(name, value());
        } else if (name == "--message-size") {
            config.message_size_bytes = parse_size(name, value());
        } else if (name == "--latency-sample-every") {
            config.latency_sample_every = parse_size(name, value());
        } else if (name == "--max-errors") {
            config.max_errors = parse_size(name, value());
        } else if (name == "--auto-split-poll-interval") {
            config.auto_split_poll_interval = parse_duration(value());
        } else if (name == "--anonymous") {
            config.anonymous = inline_value ? parse_bool(*inline_value) : true;
        } else if (name == "--auto-split") {
            config.auto_split = inline_value ? parse_bool(*inline_value) : true;
        } else if (name == "--skip-table-write") {
            config.skip_table_write =
                inline_value ? parse_bool(*inline_value) : true;
        } else {
            throw std::runtime_error("unknown argument: " + name);
        }
    }

    if (config.dsn.empty()) {
        throw std::runtime_error("--dsn is required");
    }
    if (config.topic_path.empty()) {
        throw std::runtime_error("--topic is required");
    }
    if (!config.skip_table_write && config.table_path.empty()) {
        throw std::runtime_error(
            "--table is required unless --skip-table-write is set");
    }
    if (!config.skip_table_write && config.table_path.find('`') !=
                                        std::string::npos) {
        throw std::runtime_error("--table cannot contain a backtick");
    }
    if (config.run_id.empty()) {
        throw std::runtime_error("--run-id cannot be empty");
    }
    if (config.auto_split &&
        (config.mode != WriterMode::Many ||
         config.routing != RoutingMode::BoundedKey)) {
        throw std::runtime_error(
            "--auto-split requires --mode=many --routing=bounded-key");
    }
    if (config.duration <= Milliseconds::zero() ||
        config.warmup < Milliseconds::zero() ||
        config.transaction_timeout <= Milliseconds::zero() ||
        config.concurrency == 0 || config.messages_per_transaction == 0 ||
        config.latency_sample_every == 0 || config.max_errors == 0 ||
        config.auto_split_poll_interval <= Milliseconds::zero()) {
        throw std::runtime_error("numeric benchmark values must be positive");
    }
    return config;
}

std::uint32_t murmur2_32(std::string_view data,
                         std::uint32_t seed = kafka_hash_seed) {
    constexpr std::uint32_t multiplier = 0x5bd1e995U;
    constexpr std::uint32_t shift = 24U;
    auto hash = seed ^ static_cast<std::uint32_t>(data.size());
    std::size_t index = 0;
    while (index + 4 <= data.size()) {
        const auto* bytes = reinterpret_cast<const unsigned char*>(
            data.data() + static_cast<std::ptrdiff_t>(index));
        auto block = static_cast<std::uint32_t>(bytes[0]) |
                     (static_cast<std::uint32_t>(bytes[1]) << 8U) |
                     (static_cast<std::uint32_t>(bytes[2]) << 16U) |
                     (static_cast<std::uint32_t>(bytes[3]) << 24U);
        block *= multiplier;
        block ^= block >> shift;
        block *= multiplier;
        hash *= multiplier;
        hash ^= block;
        index += 4;
    }
    const auto* tail = reinterpret_cast<const unsigned char*>(
        data.data() + static_cast<std::ptrdiff_t>(index));
    switch (data.size() - index) {
        case 3:
            hash ^= static_cast<std::uint32_t>(tail[2]) << 16U;
            [[fallthrough]];
        case 2:
            hash ^= static_cast<std::uint32_t>(tail[1]) << 8U;
            [[fallthrough]];
        case 1:
            hash ^= static_cast<std::uint32_t>(tail[0]);
            hash *= multiplier;
            break;
        default:
            break;
    }
    hash ^= hash >> 13U;
    hash *= multiplier;
    hash ^= hash >> 15U;
    return hash;
}

struct Partition {
    std::uint32_t id = 0;
    std::string from_bound;
    std::optional<std::string> to_bound;
};

struct Topology {
    std::vector<Partition> active;
    std::size_t total_partition_count = 0;
};

Topology read_topology(TTopicClient& client, const std::string& topic_path) {
    const auto result = client.DescribeTopic(topic_path).GetValueSync();
    if (!result.IsSuccess()) {
        throw std::runtime_error(status_error("describe topic", result));
    }
    Topology topology;
    const auto& description = result.GetTopicDescription();
    topology.total_partition_count = description.GetPartitions().size();
    for (const auto& partition : description.GetPartitions()) {
        if (!partition.GetActive()) {
            continue;
        }
        topology.active.push_back(Partition{
            .id = static_cast<std::uint32_t>(partition.GetPartitionId()),
            .from_bound = partition.GetFromBound().value_or(""),
            .to_bound = partition.GetToBound(),
        });
    }
    if (topology.active.empty()) {
        throw std::runtime_error("topic has no active partitions");
    }
    std::sort(topology.active.begin(),
              topology.active.end(),
              [](const Partition& left, const Partition& right) {
                  return left.id < right.id;
              });
    return topology;
}

std::vector<std::uint32_t> partition_ids(const Topology& topology) {
    std::vector<std::uint32_t> result;
    result.reserve(topology.active.size());
    for (const auto& partition : topology.active) {
        result.push_back(partition.id);
    }
    return result;
}

class TopologyState {
public:
    explicit TopologyState(Topology initial)
        : initial_(initial), current_(std::move(initial)) {}

    Topology snapshot() const {
        std::lock_guard lock(mutex_);
        return current_;
    }

    bool is_active(std::uint32_t partition_id) const {
        std::lock_guard lock(mutex_);
        return std::any_of(
            current_.active.begin(),
            current_.active.end(),
            [partition_id](const Partition& partition) {
                return partition.id == partition_id;
            });
    }

    void record(Topology topology, Clock::time_point now) {
        std::lock_guard lock(mutex_);
        ++describe_calls_;
        if (!split_observed_ &&
            topology.active.size() > initial_.active.size()) {
            split_observed_ = true;
            first_split_after_ =
                std::chrono::duration_cast<Milliseconds>(now - started_at_);
        }
        current_ = std::move(topology);
    }

    void record_error(std::string error) {
        std::lock_guard lock(mutex_);
        ++describe_calls_;
        last_error_ = std::move(error);
    }

    void start(Clock::time_point value) {
        std::lock_guard lock(mutex_);
        started_at_ = value;
    }

    nlohmann::json report(const std::string& path,
                          bool auto_split_enabled) const {
        std::lock_guard lock(mutex_);
        nlohmann::json result = {
            {"path", path},
            {"active_partition_count", current_.active.size()},
            {"active_partition_ids", partition_ids(current_)},
            {"initial_active_partition_count", initial_.active.size()},
            {"initial_active_partition_ids", partition_ids(initial_)},
            {"total_partition_count", current_.total_partition_count},
            {"auto_split_enabled", auto_split_enabled},
            {"auto_split_observed", split_observed_},
            {"topology_describe_calls", describe_calls_},
        };
        if (split_observed_) {
            result["first_split_after_ms"] = first_split_after_.count();
        }
        if (!last_error_.empty()) {
            result["topology_last_error"] = last_error_;
        }
        return result;
    }

private:
    mutable std::mutex mutex_;
    Topology initial_;
    Topology current_;
    Clock::time_point started_at_ = Clock::now();
    bool split_observed_ = false;
    Milliseconds first_split_after_{0};
    std::uint64_t describe_calls_ = 0;
    std::string last_error_;
};

std::uint32_t choose_partition(const Topology& topology,
                               RoutingMode routing,
                               std::string_view key,
                               std::size_t worker_id,
                               std::uint64_t message_sequence) {
    if (topology.active.empty()) {
        throw std::runtime_error("cannot choose from an empty topology");
    }
    if (routing == RoutingMode::PartitionId) {
        const auto index =
            (message_sequence + static_cast<std::uint64_t>(worker_id) - 1U) %
            topology.active.size();
        return topology.active[index].id;
    }
    if (routing == RoutingMode::Key) {
        auto by_id = topology.active;
        std::sort(by_id.begin(),
                  by_id.end(),
                  [](const Partition& left, const Partition& right) {
                      return left.id < right.id;
                  });
        const auto index =
            (murmur2_32(key) & kafka_hash_mask) % by_id.size();
        return by_id[index].id;
    }

    const auto hashed =
        NYdb::NTopic::TProducerSettings::DefaultPartitioningKeyHasher(key);
    auto by_bound = topology.active;
    std::sort(by_bound.begin(),
              by_bound.end(),
              [](const Partition& left, const Partition& right) {
                  return left.from_bound < right.from_bound;
              });
    const auto upper = std::upper_bound(
        by_bound.begin(),
        by_bound.end(),
        hashed,
        [](const std::string& value, const Partition& partition) {
            return value < partition.from_bound;
        });
    if (upper == by_bound.begin()) {
        throw std::runtime_error("invalid bounded Topic topology");
    }
    return std::prev(upper)->id;
}

struct LifecycleSnapshot {
    std::uint64_t describe_topic_calls = 0;
    std::uint64_t stream_write_opens = 0;
    std::uint64_t writer_init_attempts = 0;
    std::uint64_t writer_init_errors = 0;
    std::uint64_t writer_close_events = 0;
    std::uint64_t writer_close_errors = 0;
    std::uint64_t write_requests = 0;
    std::uint64_t write_request_messages = 0;
    std::uint64_t write_request_errors = 0;
    std::uint64_t acknowledged_messages = 0;
    std::uint64_t written_in_tx_messages = 0;
    std::uint64_t skipped_messages = 0;
};

class Lifecycle {
public:
    LifecycleSnapshot snapshot() const {
        return {
            .describe_topic_calls = describe_topic_calls_.load(),
            .stream_write_opens = stream_write_opens_.load(),
            .writer_init_attempts = writer_init_attempts_.load(),
            .writer_init_errors = writer_init_errors_.load(),
            .writer_close_events = writer_close_events_.load(),
            .writer_close_errors = writer_close_errors_.load(),
            .write_requests = write_requests_.load(),
            .write_request_messages = write_request_messages_.load(),
            .write_request_errors = write_request_errors_.load(),
            .acknowledged_messages = acknowledged_messages_.load(),
            .written_in_tx_messages = written_in_tx_messages_.load(),
            .skipped_messages = skipped_messages_.load(),
        };
    }

    void writer_init_attempt() { ++writer_init_attempts_; }
    void writer_init_success() { ++stream_write_opens_; }
    void writer_init_error() { ++writer_init_errors_; }
    void writer_close(bool success) {
        ++writer_close_events_;
        if (!success) {
            ++writer_close_errors_;
        }
    }
    void write_request() {
        ++write_requests_;
        ++write_request_messages_;
    }
    void write_error() { ++write_request_errors_; }
    void ack_written_in_tx() {
        ++acknowledged_messages_;
        ++written_in_tx_messages_;
    }
    void ack_skipped() {
        ++acknowledged_messages_;
        ++skipped_messages_;
    }

private:
    std::atomic<std::uint64_t> describe_topic_calls_{0};
    std::atomic<std::uint64_t> stream_write_opens_{0};
    std::atomic<std::uint64_t> writer_init_attempts_{0};
    std::atomic<std::uint64_t> writer_init_errors_{0};
    std::atomic<std::uint64_t> writer_close_events_{0};
    std::atomic<std::uint64_t> writer_close_errors_{0};
    std::atomic<std::uint64_t> write_requests_{0};
    std::atomic<std::uint64_t> write_request_messages_{0};
    std::atomic<std::uint64_t> write_request_errors_{0};
    std::atomic<std::uint64_t> acknowledged_messages_{0};
    std::atomic<std::uint64_t> written_in_tx_messages_{0};
    std::atomic<std::uint64_t> skipped_messages_{0};
};

LifecycleSnapshot subtract(const LifecycleSnapshot& after,
                           const LifecycleSnapshot& before) {
#define SUBTRACT_FIELD(name) .name = after.name - before.name
    return {
        SUBTRACT_FIELD(describe_topic_calls),
        SUBTRACT_FIELD(stream_write_opens),
        SUBTRACT_FIELD(writer_init_attempts),
        SUBTRACT_FIELD(writer_init_errors),
        SUBTRACT_FIELD(writer_close_events),
        SUBTRACT_FIELD(writer_close_errors),
        SUBTRACT_FIELD(write_requests),
        SUBTRACT_FIELD(write_request_messages),
        SUBTRACT_FIELD(write_request_errors),
        SUBTRACT_FIELD(acknowledged_messages),
        SUBTRACT_FIELD(written_in_tx_messages),
        SUBTRACT_FIELD(skipped_messages),
    };
#undef SUBTRACT_FIELD
}

nlohmann::json lifecycle_json(const LifecycleSnapshot& value) {
    return {
        {"describe_topic_calls", value.describe_topic_calls},
        {"stream_write_opens", value.stream_write_opens},
        {"writer_init_attempts", value.writer_init_attempts},
        {"writer_init_errors", value.writer_init_errors},
        {"writer_close_events", value.writer_close_events},
        {"writer_close_errors", value.writer_close_errors},
        {"write_requests", value.write_requests},
        {"write_request_messages", value.write_request_messages},
        {"write_request_errors", value.write_request_errors},
        {"acknowledged_messages", value.acknowledged_messages},
        {"written_in_tx_messages", value.written_in_tx_messages},
        {"skipped_messages", value.skipped_messages},
    };
}

struct WriteOutcome {
    TStatus status;
    bool session_closed = false;
};

class TransactionalSession {
public:
    TransactionalSession(TTopicClient& client,
                         const Config& config,
                         TopologyState& topology,
                         std::size_t worker_id,
                         std::optional<std::uint32_t> partition_id,
                         Lifecycle& lifecycle)
        : lifecycle_(lifecycle)
        , topology_(topology)
        , partition_id_(partition_id)
        , event_timeout_(config.transaction_timeout)
        , topology_poll_interval_(config.auto_split
                                      ? config.auto_split_poll_interval
                                      : config.transaction_timeout) {
        lifecycle_.writer_init_attempt();
        auto settings = NYdb::NTopic::TWriteSessionSettings();
        settings.Path(config.topic_path)
            .DirectWriteToPartition(false)
            .Codec(NYdb::NTopic::ECodec::RAW)
            .BatchFlushMessageCount(1);
        if (!config.producer_id_prefix.empty()) {
            auto producer_id = config.producer_id_prefix + "-slot-" +
                               std::to_string(worker_id);
            if (partition_id) {
                producer_id += "-partition-" + std::to_string(*partition_id);
            }
            settings.ProducerId(producer_id);
        }
        if (partition_id) {
            settings.PartitionId(*partition_id);
        }
        writer_ = client.CreateWriteSession(settings);
        if (!writer_) {
            lifecycle_.writer_init_error();
            throw std::runtime_error("failed to create Topic write session");
        }
        try {
            wait_for_initial_token();
            lifecycle_.writer_init_success();
        } catch (...) {
            lifecycle_.writer_init_error();
            throw;
        }
    }

    ~TransactionalSession() { close(); }

    TransactionalSession(const TransactionalSession&) = delete;
    TransactionalSession& operator=(const TransactionalSession&) = delete;

    WriteOutcome write(TWriteMessage message,
                       NYdb::NQuery::TTransaction& transaction) {
        if (!token_) {
            lifecycle_.write_error();
            return {local_error(EStatus::UNAVAILABLE,
                                "Topic writer has no continuation token"),
                    true};
        }
        lifecycle_.write_request();
        writer_->Write(std::move(*token_), std::move(message), &transaction);
        token_.reset();

        bool acknowledged = false;
        while (!acknowledged || !token_) {
            const auto wait_result = wait_for_event();
            if (wait_result == WaitResult::PartitionInactive) {
                lifecycle_.write_error();
                return {local_error(EStatus::UNAVAILABLE,
                                    "Topic partition became inactive"),
                        true};
            }
            if (wait_result == WaitResult::TimedOut) {
                lifecycle_.write_error();
                return {local_error(EStatus::CLIENT_DEADLINE_EXCEEDED,
                                    "Timed out waiting for Topic writer event"),
                        true};
            }
            for (auto& event : writer_->GetEvents(false)) {
                if (auto* ready =
                        std::get_if<WriteEvent::TReadyToAcceptEvent>(&event)) {
                    token_ = std::move(ready->ContinuationToken);
                    continue;
                }
                if (auto* acknowledgements =
                        std::get_if<WriteEvent::TAcksEvent>(&event)) {
                    for (const auto& acknowledgement : acknowledgements->Acks) {
                        if (acknowledgement.State ==
                            WriteEvent::TWriteAck::EES_WRITTEN_IN_TX) {
                            lifecycle_.ack_written_in_tx();
                            acknowledged = true;
                        } else if (acknowledgement.State ==
                                   WriteEvent::TWriteAck::EES_ALREADY_WRITTEN) {
                            lifecycle_.ack_skipped();
                            acknowledged = true;
                        } else {
                            lifecycle_.write_error();
                            return {local_error(
                                        EStatus::UNAVAILABLE,
                                        "Topic write was discarded before "
                                        "transaction commit"),
                                    true};
                        }
                    }
                    continue;
                }
                const auto& closed =
                    std::get<NYdb::NTopic::TSessionClosedEvent>(event);
                lifecycle_.write_error();
                return {copy_status(closed), true};
            }
        }
        return {TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues{}), false};
    }

    void close() noexcept {
        if (!writer_) {
            return;
        }
        try {
            const auto success = writer_->Close(TDuration::Seconds(10));
            lifecycle_.writer_close(success);
        } catch (...) {
            lifecycle_.writer_close(false);
        }
        writer_.reset();
        token_.reset();
    }

private:
    enum class WaitResult { EventReady, PartitionInactive, TimedOut };

    WaitResult wait_for_event() {
        const auto deadline = Clock::now() + event_timeout_;
        for (;;) {
            const auto now = Clock::now();
            if (now >= deadline) {
                return WaitResult::TimedOut;
            }
            auto remaining = std::chrono::duration_cast<Milliseconds>(
                deadline - now);
            remaining = std::max(remaining, Milliseconds{1});
            const auto wait_duration =
                std::min(remaining, topology_poll_interval_);
            if (writer_->WaitEvent().Wait(TDuration::MilliSeconds(
                    static_cast<std::uint64_t>(wait_duration.count())))) {
                return WaitResult::EventReady;
            }
            if (partition_id_ && !topology_.is_active(*partition_id_)) {
                return WaitResult::PartitionInactive;
            }
        }
    }

    void wait_for_initial_token() {
        for (;;) {
            const auto wait_result = wait_for_event();
            if (wait_result == WaitResult::PartitionInactive) {
                throw std::runtime_error(
                    "Topic partition became inactive while opening writer");
            }
            if (wait_result == WaitResult::TimedOut) {
                throw std::runtime_error(
                    "timed out waiting for Topic writer initialization");
            }
            const auto event = writer_->GetEvent(false);
            if (!event) {
                continue;
            }
            if (auto* ready =
                    std::get_if<WriteEvent::TReadyToAcceptEvent>(&*event)) {
                token_ = std::move(ready->ContinuationToken);
                return;
            }
            if (const auto* closed =
                    std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                throw std::runtime_error(
                    status_error("initialize Topic writer", *closed));
            }
        }
    }

    Lifecycle& lifecycle_;
    TopologyState& topology_;
    std::optional<std::uint32_t> partition_id_;
    Milliseconds event_timeout_;
    Milliseconds topology_poll_interval_;
    std::shared_ptr<IWriteSession> writer_;
    std::optional<TContinuationToken> token_;
};

struct PoolWriteOutcome {
    TStatus status;
    Nanoseconds acquire{0};
    Nanoseconds write{0};
};

class WriterPool {
public:
    WriterPool(TTopicClient& client,
               const Config& config,
               TopologyState& topology,
               std::size_t worker_id,
               Lifecycle& lifecycle)
        : client_(client)
        , config_(config)
        , topology_(topology)
        , worker_id_(worker_id)
        , lifecycle_(lifecycle) {}

    PoolWriteOutcome write(std::optional<std::uint32_t> partition_id,
                           TWriteMessage message,
                           NYdb::NQuery::TTransaction& transaction) {
        const auto acquire_started_at = Clock::now();
        const auto key = partition_id ? static_cast<std::int64_t>(*partition_id)
                                      : std::int64_t{-1};
        auto found = sessions_.find(key);
        if (found == sessions_.end()) {
            try {
                found = sessions_
                            .emplace(key,
                                     std::make_unique<TransactionalSession>(
                                         client_,
                                         config_,
                                         topology_,
                                         worker_id_,
                                         partition_id,
                                         lifecycle_))
                            .first;
            } catch (const std::exception& error) {
                return {
                    .status = local_error(EStatus::UNAVAILABLE, error.what()),
                    .acquire = std::chrono::duration_cast<Nanoseconds>(
                        Clock::now() - acquire_started_at),
                };
            }
        }
        const auto write_started_at = Clock::now();
        auto outcome = found->second->write(std::move(message), transaction);
        const auto write_finished_at = Clock::now();
        if (outcome.session_closed) {
            sessions_.erase(found);
        }
        return {
            .status = std::move(outcome.status),
            .acquire = std::chrono::duration_cast<Nanoseconds>(
                write_started_at - acquire_started_at),
            .write = std::chrono::duration_cast<Nanoseconds>(
                write_finished_at - write_started_at),
        };
    }

    void close() { sessions_.clear(); }

private:
    TTopicClient& client_;
    const Config& config_;
    TopologyState& topology_;
    std::size_t worker_id_;
    Lifecycle& lifecycle_;
    std::map<std::int64_t, std::unique_ptr<TransactionalSession>> sessions_;
};

struct Timings {
    Nanoseconds table{0};
    Nanoseconds writer_start{0};
    Nanoseconds writer_write{0};
};

struct WorkerStats {
    std::uint64_t logical_transactions = 0;
    std::uint64_t committed = 0;
    std::uint64_t failed = 0;
    std::uint64_t cancelled = 0;
    std::uint64_t attempts = 0;
    std::uint64_t retries = 0;
    std::uint64_t messages = 0;
    std::uint64_t bytes = 0;
    std::string first_error;
    std::vector<Nanoseconds> transaction_latency;
    std::vector<Nanoseconds> table_latency;
    std::vector<Nanoseconds> writer_start_latency;
    std::vector<Nanoseconds> writer_write_latency;
};

struct WorkerContext {
    WorkerContext(TTopicClient& topic_client,
                  const Config& config,
                  TopologyState& topology,
                  std::size_t worker_id,
                  Lifecycle& lifecycle)
        : writer_pool(
              topic_client, config, topology, worker_id, lifecycle) {}

    WriterPool writer_pool;
    std::atomic<std::uint64_t> sequence{0};
};

std::string make_payload(std::size_t size) {
    std::string payload(size, '\0');
    for (std::size_t index = 0; index < size; ++index) {
        payload[index] = static_cast<char>('a' + (index % 26));
    }
    return payload;
}

std::string message_key(std::size_t worker_id,
                        std::uint64_t message_sequence) {
    return "worker-" + std::to_string(worker_id) + "-message-" +
           std::to_string(message_sequence);
}

NYdb::TParams table_parameters(const Config& config,
                               std::size_t worker_id,
                               std::uint64_t sequence) {
    return NYdb::TParamsBuilder()
        .AddParam("$run_id")
        .Utf8(config.run_id)
        .Build()
        .AddParam("$worker_id")
        .Uint64(worker_id)
        .Build()
        .AddParam("$seq_no")
        .Uint64(sequence)
        .Build()
        .Build();
}

struct TransactionResult {
    Nanoseconds latency{0};
    Timings timings;
    std::size_t attempts = 0;
    TStatus status{EStatus::SUCCESS, NYdb::NIssue::TIssues{}};
};

TransactionResult execute_transaction(TQueryClient& query_client,
                                      const Config& config,
                                      TopologyState& topology_state,
                                      WorkerContext& worker,
                                      std::size_t worker_id,
                                      std::uint64_t sequence,
                                      const std::string& payload) {
    const auto started_at = Clock::now();
    TransactionResult output;
    const auto query =
        "DECLARE $run_id AS Utf8;\n"
        "DECLARE $worker_id AS Uint64;\n"
        "DECLARE $seq_no AS Uint64;\n"
        "UPSERT INTO `" +
        config.table_path +
        "` (run_id, worker_id, seq_no, updated_at) "
        "VALUES ($run_id, $worker_id, $seq_no, CurrentUtcTimestamp());";

    auto retry_settings = NYdb::NQuery::TRetryOperationSettings()
                              .Idempotent(true)
                              .MaxTimeout(TDuration::MilliSeconds(
                                  config.transaction_timeout.count()));
    if (!config.query_retries) {
        retry_settings.MaxRetries(0);
    }

    output.status = query_client.RetryQuerySync(
        [&](NYdb::NQuery::TSession session) -> TStatus {
            ++output.attempts;
            Timings timings;
            auto begin = session
                             .BeginTransaction(
                                 NYdb::NQuery::TTxSettings::SerializableRW())
                             .GetValueSync();
            if (!begin.IsSuccess()) {
                return copy_status(begin);
            }
            auto transaction = begin.GetTransaction();

            if (!config.skip_table_write) {
                const auto table_started_at = Clock::now();
                const auto result = session
                                        .ExecuteQuery(
                                            query,
                                            NYdb::NQuery::TTxControl::Tx(
                                                transaction),
                                            table_parameters(
                                                config, worker_id, sequence))
                                        .GetValueSync();
                timings.table = std::chrono::duration_cast<Nanoseconds>(
                    Clock::now() - table_started_at);
                if (!result.IsSuccess()) {
                    output.timings = timings;
                    return copy_status(result);
                }
            }

            const auto topology = topology_state.snapshot();
            for (std::size_t message_index = 0;
                 message_index < config.messages_per_transaction;
                 ++message_index) {
                const auto message_sequence =
                    (sequence - 1U) * config.messages_per_transaction +
                    message_index + 1U;
                const auto key = message_key(worker_id, message_sequence);
                std::optional<std::uint32_t> partition_id;
                if (config.mode == WriterMode::Many) {
                    partition_id = choose_partition(topology,
                                                    config.routing,
                                                    key,
                                                    worker_id,
                                                    message_sequence);
                }
                auto message = TWriteMessage(payload);
                if (!config.auto_seq_no) {
                    message.SeqNo(message_sequence);
                }

                // A cache miss includes opening the underlying StreamWrite
                // session; a hit measures just the pool lookup.
                auto outcome = worker.writer_pool.write(
                    partition_id, std::move(message), transaction);
                timings.writer_start += outcome.acquire;
                timings.writer_write += outcome.write;
                if (!outcome.status.IsSuccess()) {
                    output.timings = timings;
                    return outcome.status;
                }
            }

            const auto commit = transaction.Commit().GetValueSync();
            output.timings = timings;
            return copy_status(commit);
        },
        retry_settings);
    output.latency =
        std::chrono::duration_cast<Nanoseconds>(Clock::now() - started_at);
    return output;
}

void run_worker(TQueryClient& query_client,
                const Config& config,
                TopologyState& topology,
                WorkerContext& context,
                std::size_t worker_id,
                const std::string& payload,
                Clock::time_point deadline,
                std::atomic<std::size_t>& final_errors,
                std::atomic<bool>& abort_phase,
                WorkerStats& stats) {
    while (!interrupted.load() && !abort_phase.load() &&
           Clock::now() < deadline) {
        const auto sequence = ++context.sequence;
        ++stats.logical_transactions;
        auto result = execute_transaction(query_client,
                                          config,
                                          topology,
                                          context,
                                          worker_id,
                                          sequence,
                                          payload);
        stats.attempts += result.attempts;
        if (result.attempts > 1) {
            stats.retries += result.attempts - 1;
        }
        if (!result.status.IsSuccess()) {
            ++stats.failed;
            if (stats.first_error.empty()) {
                stats.first_error =
                    status_error("execute transaction", result.status);
            }
            if (++final_errors >= config.max_errors) {
                abort_phase.store(true);
            }
            continue;
        }

        ++stats.committed;
        stats.messages += config.messages_per_transaction;
        stats.bytes += config.messages_per_transaction * payload.size();
        if (sequence % config.latency_sample_every == 0) {
            stats.transaction_latency.push_back(result.latency);
            if (!config.skip_table_write) {
                stats.table_latency.push_back(result.timings.table);
            }
            stats.writer_start_latency.push_back(result.timings.writer_start);
            stats.writer_write_latency.push_back(result.timings.writer_write);
        }
    }
}

struct PhaseStats : WorkerStats {
    Milliseconds duration{0};
    bool aborted = false;
};

void merge_vector(std::vector<Nanoseconds>& destination,
                  std::vector<Nanoseconds>& source) {
    destination.insert(destination.end(), source.begin(), source.end());
}

PhaseStats run_phase(TQueryClient& query_client,
                     const Config& config,
                     TopologyState& topology,
                     std::vector<std::unique_ptr<WorkerContext>>& contexts,
                     const std::string& payload,
                     Milliseconds duration) {
    if (duration == Milliseconds::zero()) {
        return {};
    }
    const auto started_at = Clock::now();
    const auto deadline = started_at + duration;
    std::atomic<std::size_t> final_errors{0};
    std::atomic<bool> abort_phase{false};
    std::vector<WorkerStats> workers(config.concurrency);
    std::vector<std::thread> threads;
    threads.reserve(config.concurrency);
    for (std::size_t worker_id = 0; worker_id < config.concurrency;
         ++worker_id) {
        threads.emplace_back(run_worker,
                             std::ref(query_client),
                             std::cref(config),
                             std::ref(topology),
                             std::ref(*contexts[worker_id]),
                             worker_id,
                             std::cref(payload),
                             deadline,
                             std::ref(final_errors),
                             std::ref(abort_phase),
                             std::ref(workers[worker_id]));
    }
    for (auto& thread : threads) {
        thread.join();
    }

    PhaseStats merged;
    merged.duration =
        std::chrono::duration_cast<Milliseconds>(Clock::now() - started_at);
    merged.aborted = abort_phase.load();
    for (auto& stats : workers) {
        merged.logical_transactions += stats.logical_transactions;
        merged.committed += stats.committed;
        merged.failed += stats.failed;
        merged.cancelled += stats.cancelled;
        merged.attempts += stats.attempts;
        merged.retries += stats.retries;
        merged.messages += stats.messages;
        merged.bytes += stats.bytes;
        if (merged.first_error.empty()) {
            merged.first_error = stats.first_error;
        }
        merge_vector(merged.transaction_latency, stats.transaction_latency);
        merge_vector(merged.table_latency, stats.table_latency);
        merge_vector(merged.writer_start_latency,
                     stats.writer_start_latency);
        merge_vector(merged.writer_write_latency,
                     stats.writer_write_latency);
    }
    return merged;
}

nlohmann::json latency_summary(std::vector<Nanoseconds> samples) {
    if (samples.empty()) {
        return {{"count", 0},
                {"min_ms", 0.0},
                {"mean_ms", 0.0},
                {"p50_ms", 0.0},
                {"p95_ms", 0.0},
                {"p99_ms", 0.0},
                {"max_ms", 0.0}};
    }
    std::sort(samples.begin(), samples.end());
    const auto percentile = [&](double value) {
        const auto rank = std::max<std::size_t>(
            1, static_cast<std::size_t>(std::ceil(value * samples.size())));
        return static_cast<double>(samples[rank - 1].count()) / 1'000'000.0;
    };
    long double total = 0.0;
    for (const auto sample : samples) {
        total += static_cast<long double>(sample.count());
    }
    return {
        {"count", samples.size()},
        {"min_ms",
         static_cast<double>(samples.front().count()) / 1'000'000.0},
        {"mean_ms",
         static_cast<double>(total / samples.size()) / 1'000'000.0},
        {"p50_ms", percentile(0.50)},
        {"p95_ms", percentile(0.95)},
        {"p99_ms", percentile(0.99)},
        {"max_ms",
         static_cast<double>(samples.back().count()) / 1'000'000.0},
    };
}

nlohmann::json phase_json(const PhaseStats& stats, bool skip_table_write) {
    const auto seconds =
        static_cast<double>(stats.duration.count()) / 1000.0;
    nlohmann::json latency = {
        {"transaction", latency_summary(stats.transaction_latency)},
        {"writer_start", latency_summary(stats.writer_start_latency)},
        {"writer_write", latency_summary(stats.writer_write_latency)},
    };
    if (!skip_table_write) {
        latency["table_exec"] = latency_summary(stats.table_latency);
    }
    nlohmann::json result = {
        {"duration_seconds", seconds},
        {"logical_transactions", stats.logical_transactions},
        {"committed_transactions", stats.committed},
        {"failed_transactions", stats.failed},
        {"cancelled_transactions", stats.cancelled},
        {"transaction_attempts", stats.attempts},
        {"retries", stats.retries},
        {"committed_messages", stats.messages},
        {"committed_payload_bytes", stats.bytes},
        {"transactions_per_second",
         seconds == 0.0 ? 0.0 : stats.committed / seconds},
        {"messages_per_second",
         seconds == 0.0 ? 0.0 : stats.messages / seconds},
        {"payload_mib_per_second",
         seconds == 0.0
             ? 0.0
             : stats.bytes / (1024.0 * 1024.0) / seconds},
        {"aborted_by_error_limit", stats.aborted},
        {"latency_ms", std::move(latency)},
    };
    if (!stats.first_error.empty()) {
        result["first_error"] = stats.first_error;
    }
    return result;
}

nlohmann::json config_json(const Config& config) {
    return {
        {"run_id", config.run_id},
        {"label", config.label},
        {"mode", to_string(config.mode)},
        {"routing", to_string(config.routing)},
        {"auto_seq_no", config.auto_seq_no},
        {"duration", duration_string(config.duration)},
        {"warmup", duration_string(config.warmup)},
        {"transaction_timeout",
         duration_string(config.transaction_timeout)},
        {"concurrency", config.concurrency},
        {"messages_per_transaction", config.messages_per_transaction},
        {"message_size_bytes", config.message_size_bytes},
        {"latency_sample_every", config.latency_sample_every},
        {"max_errors", config.max_errors},
        {"skip_table_write", config.skip_table_write},
        {"producer_id_prefix", config.producer_id_prefix},
        {"stable_producer_slots", !config.producer_id_prefix.empty()},
        {"query_retries", config.query_retries},
        {"direct_write", false},
        {"auto_split", config.auto_split},
        {"auto_split_poll_interval",
         duration_string(config.auto_split_poll_interval)},
        {"multiwriter_implementation",
         config.mode == WriterMode::Many ? "session_pool_by_partition"
                                         : "not_applicable"},
        {"session_lifecycle", "pooled_per_worker"},
    };
}

NYdb::TDriverConfig driver_config(const Config& config) {
    auto result = config.anonymous ? NYdb::TDriverConfig(config.dsn)
                                   : NYdb::CreateFromEnvironment(config.dsn);
    result.SetClientThreadsNum(std::max<std::size_t>(2, config.concurrency));
    return result;
}

int run_benchmark(const Config& config) {
    auto driver = NYdb::TDriver(driver_config(config));
    auto query_settings = NYdb::NQuery::TClientSettings();
    query_settings.SessionPoolSettings(
        NYdb::NQuery::TSessionPoolSettings()
            .MaxActiveSessions(config.concurrency)
            .MinPoolSize(config.concurrency));
    auto query_client = TQueryClient(driver, query_settings);
    auto topic_client = TTopicClient(driver);

    auto topology = std::make_shared<TopologyState>(
        read_topology(topic_client, config.topic_path));
    topology->start(Clock::now());
    Lifecycle lifecycle;
    std::vector<std::unique_ptr<WorkerContext>> contexts;
    contexts.reserve(config.concurrency);
    for (std::size_t worker_id = 0; worker_id < config.concurrency;
         ++worker_id) {
        contexts.push_back(std::make_unique<WorkerContext>(
            topic_client, config, *topology, worker_id, lifecycle));
    }

    std::atomic<bool> stop_monitor{false};
    std::mutex monitor_mutex;
    std::condition_variable monitor_condition;
    std::thread monitor;
    if (config.auto_split) {
        monitor = std::thread([&] {
            auto monitor_driver = NYdb::TDriver(driver_config(config));
            auto monitor_client = TTopicClient(monitor_driver);
            std::unique_lock lock(monitor_mutex);
            while (!stop_monitor.load()) {
                if (monitor_condition.wait_for(
                        lock,
                        config.auto_split_poll_interval,
                        [&] { return stop_monitor.load(); })) {
                    break;
                }
                lock.unlock();
                try {
                    topology->record(
                        read_topology(monitor_client, config.topic_path),
                        Clock::now());
                } catch (const std::exception& error) {
                    topology->record_error(error.what());
                }
                lock.lock();
            }
            monitor_driver.Stop(true);
        });
    }

    const auto stop_topology_monitor = [&] {
        if (!monitor.joinable()) {
            return;
        }
        stop_monitor.store(true);
        monitor_condition.notify_all();
        monitor.join();
        try {
            topology->record(read_topology(topic_client, config.topic_path),
                             Clock::now());
        } catch (const std::exception& error) {
            topology->record_error(error.what());
        }
    };

    const auto payload = make_payload(config.message_size_bytes);
    const auto initial = topology->snapshot();
    std::cerr << "benchmark implementation=cpp_sdk mode="
              << to_string(config.mode) << " routing="
              << to_string(config.routing) << " concurrency="
              << config.concurrency << " active_partitions="
              << initial.active.size() << '\n';

    if (config.warmup > Milliseconds::zero()) {
        std::cerr << "warmup for " << duration_string(config.warmup) << '\n';
        const auto warmup = run_phase(query_client,
                                      config,
                                      *topology,
                                      contexts,
                                      payload,
                                      config.warmup);
        if (warmup.aborted || warmup.failed != 0) {
            stop_topology_monitor();
            throw std::runtime_error(
                "warmup failed after " + std::to_string(warmup.committed) +
                " commits: " + warmup.first_error);
        }
        std::cerr << "warmup committed=" << warmup.committed << '\n';
    }

    const auto lifecycle_before = lifecycle.snapshot();
    std::cerr << "measurement for " << duration_string(config.duration)
              << '\n';
    const auto measurement = run_phase(query_client,
                                       config,
                                       *topology,
                                       contexts,
                                       payload,
                                       config.duration);
    stop_topology_monitor();
    const auto measured_lifecycle =
        subtract(lifecycle.snapshot(), lifecycle_before);

    nlohmann::json report = {
        {"schema_version", 1},
        {"implementation", "cpp_sdk"},
        {"generated_at", generated_at()},
        {"build",
         {{"sdk_version", YDB_CPP_SDK_VERSION},
          {"compiler", __VERSION__},
          {"cpp_standard", __cplusplus},
          {"cpus", std::thread::hardware_concurrency()}}},
        {"config", config_json(config)},
        {"topic", topology->report(config.topic_path, config.auto_split)},
        {"table",
         {{"path", config.table_path},
          {"enabled", !config.skip_table_write}}},
        {"result", phase_json(measurement, config.skip_table_write)},
        {"lifecycle", lifecycle_json(measured_lifecycle)},
        {"runtime_memory",
         {{"supported", false},
          {"reason", "portable allocation counters are not exposed by the "
                     "C++ runtime"}}},
    };
    std::cout << report.dump(2) << '\n';

    for (auto& context : contexts) {
        context->writer_pool.close();
    }
    driver.Stop(true);

    if (measurement.aborted) {
        throw std::runtime_error(
            "measurement aborted after reaching --max-errors");
    }
    if (measurement.failed != 0) {
        throw std::runtime_error("measurement completed with transaction "
                                 "failures: " +
                                 measurement.first_error);
    }
    return EXIT_SUCCESS;
}

int self_test() {
    const auto check = [](bool condition, std::string_view message) {
        if (!condition) {
            throw std::runtime_error("self-test failed: " +
                                     std::string(message));
        }
    };
    check(parse_duration("250ms") == Milliseconds(250),
          "millisecond duration parsing");
    check(parse_duration("2s") == Milliseconds(2000),
          "second duration parsing");
    check(duration_string(Milliseconds::zero()) == "0s",
          "zero duration formatting");
    check(murmur2_32("same-key") == murmur2_32("same-key"),
          "Murmur2 determinism");
    const Topology hash_topology{
        .active = {{.id = 9}, {.id = 3}, {.id = 7}},
        .total_partition_count = 3,
    };
    const auto chosen = choose_partition(
        hash_topology, RoutingMode::Key, "key", 0, 1);
    check(chosen == 3 || chosen == 7 || chosen == 9,
          "key routing returns an active partition");
    const Topology bounded_topology{
        .active = {{.id = 1, .from_bound = ""},
                   {.id = 2, .from_bound = "m"}},
        .total_partition_count = 2,
    };
    // Exercise bound ordering without depending on a particular Murmur value.
    const auto bounded = choose_partition(
        bounded_topology, RoutingMode::BoundedKey, "key", 0, 1);
    check(bounded == 1 || bounded == 2,
          "bounded-key routing returns an active partition");
    check(choose_partition(hash_topology,
                           RoutingMode::PartitionId,
                           "",
                           1,
                           1) == 3,
          "partition-id routing follows topology order");
    TopologyState topology_state(hash_topology);
    check(topology_state.is_active(3),
          "active partition lookup finds a current partition");
    topology_state.record(bounded_topology, Clock::now());
    check(!topology_state.is_active(3),
          "active partition lookup observes topology replacement");
    std::cout << "self-test passed\n";
    return EXIT_SUCCESS;
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        if (argc == 2 && std::string_view(argv[1]) == "--self-test") {
            return self_test();
        }
        std::signal(SIGINT, handle_signal);
        std::signal(SIGTERM, handle_signal);
        return run_benchmark(parse_config(argc, argv));
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
