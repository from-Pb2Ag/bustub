/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "common/logger.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, SampleTest) {
  // {
  //   LRUKReplacer lru_replacer(7, 2);

  //   // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.

  //   for (int i = 1; i < 7; i++) {
  //     lru_replacer.RecordAccess(i);
  //   }
  //   for (int i = 1; i < 6; i++) {
  //     lru_replacer.SetEvictable(i, true);
  //   }
  //   lru_replacer.SetEvictable(6, false);
  //   lru_replacer.RecordAccess(1);  // 1, 7

  //   int value;
  //   // 2, 3, 4 are evict. => rem: 1, 5.
  //   lru_replacer.Evict(&value);
  //   lru_replacer.Evict(&value);
  //   lru_replacer.Evict(&value);

  //   lru_replacer.RecordAccess(3);  // 3, 8
  //   lru_replacer.RecordAccess(4);  // 4, 9, 11
  //   lru_replacer.RecordAccess(5);  // 5, 10
  //   lru_replacer.RecordAccess(4);

  //   lru_replacer.SetEvictable(3, true);
  //   lru_replacer.SetEvictable(4, true);
  //   // now rem: 1, 3, 4, 5.

  //   lru_replacer.Evict(&value);
  //   lru_replacer.Evict(&value);
  //   lru_replacer.Evict(&value);
  // }
  {
    LRUKReplacer lru_replacer(1000, 3);

    for (int i = 0; i < 1000; i++) {
      lru_replacer.RecordAccess(i);
      lru_replacer.SetEvictable(i, true);
    }

    for (int i = 250; i < 1000; i++) {
      lru_replacer.RecordAccess(i);
      lru_replacer.SetEvictable(i, true);
    }

    for (int i = 500; i < 1000; i++) {
      lru_replacer.RecordAccess(i);
      lru_replacer.SetEvictable(i, true);
    }

    for (int i = 750; i < 1000; i++) {
      lru_replacer.RecordAccess(i);
      lru_replacer.SetEvictable(i, true);
    }

    LOG_INFO("access phase1: [0, 250) 1x, [250, 500) 2x, [500, 750) 3x, [750, 100) 4x.");

    for (int i = 250; i < 500; i++) {
      lru_replacer.SetEvictable(i, false);
    }

    for (int i = 0; i < 100; i++) {
      lru_replacer.Remove(i);
    }
    // now we have [100, 250) 1x, [500, 750) 3x, [750, 1000) 4x. => 650 evictable.
    int value;
    for (int i = 0; i < 250; i++) {
      lru_replacer.Evict(&value);
    }

    // now we have [600, 750) 3x, [750, 1000) 4x. => 400 evictable.
    for (int i = 250; i < 500; i++) {
      lru_replacer.SetEvictable(i, true);
    }

    // now we have [250, 500) 2x, [600, 750) 3x, [750, 1000) 4x. => 650 evictable.
    for (int i = 600; i < 750; i++) {
      lru_replacer.RecordAccess(i);
      lru_replacer.RecordAccess(i);
    }

    for (int i = 0; i < 600; i++) {
      lru_replacer.Evict(&value);
    }
  }
  // {
  //   LRUKReplacer lru_replacer(7, 2);

  //   // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
  //   lru_replacer.RecordAccess(1);
  //   lru_replacer.RecordAccess(2);
  //   lru_replacer.RecordAccess(3);
  //   lru_replacer.RecordAccess(4);
  //   lru_replacer.RecordAccess(5);
  //   lru_replacer.RecordAccess(6);
  //   lru_replacer.SetEvictable(1, true);
  //   lru_replacer.SetEvictable(2, true);
  //   lru_replacer.SetEvictable(3, true);
  //   lru_replacer.SetEvictable(4, true);
  //   lru_replacer.SetEvictable(5, true);
  //   lru_replacer.SetEvictable(6, false);
  //   ASSERT_EQ(5, lru_replacer.Size());

  //   // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
  //   // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
  //   lru_replacer.RecordAccess(1);

  //   // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
  //   // first based on LRU.
  //   int value;
  //   lru_replacer.Evict(&value);
  //   ASSERT_EQ(2, value);
  //   lru_replacer.Evict(&value);
  //   ASSERT_EQ(3, value);
  //   lru_replacer.Evict(&value);
  //   ASSERT_EQ(4, value);
  //   ASSERT_EQ(2, lru_replacer.Size());

  //   // Scenario: Now replacer has frames [5,1].
  //   // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
  //   lru_replacer.RecordAccess(3);
  //   lru_replacer.RecordAccess(4);
  //   lru_replacer.RecordAccess(5);
  //   lru_replacer.RecordAccess(4);
  //   lru_replacer.SetEvictable(3, true);
  //   lru_replacer.SetEvictable(4, true);
  //   ASSERT_EQ(4, lru_replacer.Size());

  //   // Scenario: continue looking for victims. We expect 3 to be evicted next.
  //   lru_replacer.Evict(&value);
  //   ASSERT_EQ(3, value);
  //   ASSERT_EQ(3, lru_replacer.Size());

  //   // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
  //   lru_replacer.SetEvictable(6, true);
  //   ASSERT_EQ(4, lru_replacer.Size());
  //   lru_replacer.Evict(&value);
  //   ASSERT_EQ(6, value);
  //   ASSERT_EQ(3, lru_replacer.Size());

  //   // Now we have [1,5,4]. Continue looking for victims.
  //   lru_replacer.SetEvictable(1, false);
  //   ASSERT_EQ(2, lru_replacer.Size());
  //   ASSERT_EQ(true, lru_replacer.Evict(&value));
  //   ASSERT_EQ(5, value);
  //   ASSERT_EQ(1, lru_replacer.Size());

  //   // Update access history for 1. Now we have [4,1]. Next victim is 4.
  //   lru_replacer.RecordAccess(1);
  //   lru_replacer.RecordAccess(1);
  //   lru_replacer.SetEvictable(1, true);
  //   ASSERT_EQ(2, lru_replacer.Size());
  //   ASSERT_EQ(true, lru_replacer.Evict(&value));
  //   ASSERT_EQ(value, 4);

  //   ASSERT_EQ(1, lru_replacer.Size());
  //   lru_replacer.Evict(&value);
  //   ASSERT_EQ(value, 1);
  //   ASSERT_EQ(0, lru_replacer.Size());

  //   // These operations should not modify size
  //   ASSERT_EQ(false, lru_replacer.Evict(&value));
  //   ASSERT_EQ(0, lru_replacer.Size());
  //   lru_replacer.Remove(1);
  //   ASSERT_EQ(0, lru_replacer.Size());
  // }
}
}  // namespace bustub
