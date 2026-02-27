#include "base/assert.h"

#include <gtest/gtest.h>

#if defined(DALAHASH_DEBUG_ASSERTS)

TEST(DebugAssertDeath, AssertPrintsMessageAndAborts) {
    ASSERT_DEATH({ ASSERT(false, "assert test message"); }, "assert test message");
}

TEST(DebugAssertDeath, AssertFmtPrintsMessageAndAborts) {
    ASSERT_DEATH({ ASSERT_FMT(false, "assert code=%d", 17); }, "assert code=17");
}

TEST(DebugAssertDeath, AssertPrintsConditionAndStackHeader) {
    ASSERT_DEATH({ ASSERT(2 + 2 == 5, "math broke"); }, "callstack");
}

#else

TEST(ReleaseAssert, CompilesOutWithoutEvaluatingConditionOrArgs) {
    int value = 1;
    ASSERT(++value == 99, "must compile out");
    ASSERT_FMT(++value == 99, "value=%d", ++value);
    EXPECT_EQ(value, 1);
}

#endif
