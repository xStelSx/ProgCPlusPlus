#pragma once

#include <optional>
#include <string>
#include <vector>
#include <functional>

struct HashNode {
    int key;
    std::string value;
    bool occupied;
    bool deleted;

    HashNode() : key(0), value(""), occupied(false), deleted(false) {}
    HashNode(int k, const std::string& v)
        : key(k), value(v), occupied(true), deleted(false) {}
};

enum class ProbingMethod {
    DOUBLE_HASHING,
    LINEAR,
    QUADRATIC
};

class HashTable {
public:
    HashTable();
    ~HashTable() = default;

    HashTable(const HashTable&) = delete;
    HashTable& operator=(const HashTable&) = delete;
    HashTable(HashTable&&) = default;
    HashTable& operator=(HashTable&&) = default;

    bool insert(int key, const std::string& value);
    void upsert(int key, const std::string& value);
    bool erase(int key);

    std::optional<std::string> find(int key) const;
    bool contains(int key) const;

    void print() const;
    size_t size() const { return size_; }
    size_t capacity() const { return table_.size(); }
    size_t collision_count() const { return collision_count_; }

    void set_probing_method(ProbingMethod method) { method_ = method; }
    void reset_collision_count() { collision_count_ = 0; }

private:
    std::vector<HashNode> table_;
    size_t size_;
    size_t capacity_;
    size_t collision_count_;
    ProbingMethod method_ = ProbingMethod::DOUBLE_HASHING;

    static const size_t INITIAL_CAPACITY = 16;
    static const double LOAD_FACTOR_THRESHOLD;

    size_t findSlotForRehash(int key);

    size_t hash1(int key) const;
    size_t hash2(int key) const;
    size_t doubleHash(int key, size_t attempt) const;
    size_t linearProbe(int key, size_t attempt) const;
    size_t quadraticProbe(int key, size_t attempt) const;
    size_t probe(int key, size_t attempt) const;
    void rehash();

    size_t findSlot(int key);
    size_t findSlot(int key) const;
};