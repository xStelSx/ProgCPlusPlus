#include "HashTable.h"

#include <algorithm>
#include <iostream>
#include <cmath>

const double HashTable::LOAD_FACTOR_THRESHOLD = 0.5;

HashTable::HashTable()
    : table_(INITIAL_CAPACITY), size_(0), capacity_(INITIAL_CAPACITY), collision_count_(0) {}

size_t HashTable::hash1(int key) const {
    return static_cast<size_t>(key) % capacity_;
}

size_t HashTable::hash2(int key) const {
    return 1 + (static_cast<size_t>(key) % (capacity_ - 1));
}

size_t HashTable::doubleHash(int key, size_t attempt) const {
    return (hash1(key) + attempt * hash2(key)) % capacity_;
}

size_t HashTable::linearProbe(int key, size_t attempt) const {
    return (hash1(key) + attempt) % capacity_;
}

size_t HashTable::quadraticProbe(int key, size_t attempt) const {
    return (hash1(key) + attempt * attempt) % capacity_;
}

size_t HashTable::probe(int key, size_t attempt) const {
    switch (method_) {
    case ProbingMethod::LINEAR:
        return linearProbe(key, attempt);
    case ProbingMethod::QUADRATIC:
        return quadraticProbe(key, attempt);
    case ProbingMethod::DOUBLE_HASHING:
    default:
        return doubleHash(key, attempt);
    }
}

void HashTable::rehash() {
    std::vector<HashNode> old_table = std::move(table_);
    size_t old_collisions = collision_count_; // Сохраняем счетчик коллизий
    capacity_ *= 2;
    table_ = std::vector<HashNode>(capacity_);
    size_t old_size = size_;
    size_ = 0;
    collision_count_ = old_collisions; // Восстанавливаем счетчик

    for (const auto& node : old_table) {
        if (node.occupied && !node.deleted) {
            // Используем внутреннюю вставку без повторного подсчета коллизий
            size_t index = findSlotForRehash(node.key);
            if (index < capacity_) {
                table_[index] = HashNode(node.key, node.value);
                size_++;
            }
        }
    }
}

// Вспомогательная функция для рехеширования без подсчета коллизий
size_t HashTable::findSlotForRehash(int key) {
    size_t attempt = 0;
    size_t index;

    do {
        index = probe(key, attempt);
        HashNode& node = table_[index];

        if (!node.occupied) {
            return index;
        }

        attempt++;
    } while (attempt < capacity_);

    return capacity_;
}

//  версия - с подсчетом коллизий
size_t HashTable::findSlot(int key) {
    size_t attempt = 0;
    size_t index;
    size_t first_index = hash1(key) % capacity_; // Запоминаем первую позицию

    do {
        index = probe(key, attempt);
        HashNode& node = table_[index];

        // Коллизия происходит ТОЛЬКО если ячейка занята ДРУГИМ ключом
        if (attempt > 0 && node.occupied && !node.deleted && node.key != key) {
            collision_count_++; // Считаем только реальные коллизии
        }

        // Если нашли свободную ячейку или ячейку с тем же ключом
        if (!node.occupied || (node.occupied && !node.deleted && node.key == key)) {
            return index;
        }

        attempt++;
    } while (attempt < capacity_);

    return capacity_;
}

// Const версия - без подсчета коллизий
size_t HashTable::findSlot(int key) const {
    size_t attempt = 0;
    size_t index;

    do {
        index = probe(key, attempt);
        const HashNode& node = table_[index];

        if (!node.occupied || (node.occupied && !node.deleted && node.key == key)) {
            return index;
        }

        attempt++;
    } while (attempt < capacity_);

    return capacity_;
}

bool HashTable::insert(int key, const std::string& value) {
    if (static_cast<double>(size_ + 1) / capacity_ > LOAD_FACTOR_THRESHOLD) {
        rehash();
    }

    size_t index = findSlot(key);
    if (index >= capacity_) {
        return false;
    }

    HashNode& node = table_[index];
    if (node.occupied && !node.deleted) {
        return false; // Key already exists
    }

    node.key = key;
    node.value = value;
    node.occupied = true;
    node.deleted = false;
    size_++;
    return true;
}

void HashTable::upsert(int key, const std::string& value) {
    if (static_cast<double>(size_ + 1) / capacity_ > LOAD_FACTOR_THRESHOLD) {
        rehash();
    }

    size_t index = findSlot(key);
    if (index >= capacity_) {
        return;
    }

    HashNode& node = table_[index];
    if (node.occupied && !node.deleted) {
        node.value = value;
    }
    else {
        node.key = key;
        node.value = value;
        node.occupied = true;
        node.deleted = false;
        size_++;
    }
}

bool HashTable::erase(int key) {
    size_t index = findSlot(key);
    if (index >= capacity_) {
        return false;
    }

    HashNode& node = table_[index];
    if (node.occupied && !node.deleted) {
        node.deleted = true;
        size_--;
        return true;
    }
    return false;
}

std::optional<std::string> HashTable::find(int key) const {
    size_t index = findSlot(key);
    if (index >= capacity_) {
        return std::nullopt;
    }

    const HashNode& node = table_[index];
    if (node.occupied && !node.deleted) {
        return node.value;
    }
    return std::nullopt;
}

bool HashTable::contains(int key) const {
    size_t index = findSlot(key);
    if (index >= capacity_) {
        return false;
    }

    const HashNode& node = table_[index];
    return node.occupied && !node.deleted;
}

void HashTable::print() const {
    std::cout << "Hash Table (Size: " << size_ << ", Capacity: " << capacity_
        << ", Collisions: " << collision_count_ << ")\n";
    for (size_t i = 0; i < capacity_; ++i) {
        const HashNode& node = table_[i];
        if (node.occupied && !node.deleted) {
            std::cout << "[" << i << "] (" << node.key << " : " << node.value << ") ";
        }
        else if (node.deleted) {
            std::cout << "[" << i << "] <deleted> ";
        }
    }
    std::cout << "\n";
}