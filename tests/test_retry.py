from tools.retry import RetryPolicy


def test_backoff_formula():
    policy = RetryPolicy(base_delay=0.5, max_delay=4.0)
    # delay = min(base_delay * 2^attempt, max_delay)（§16）
    assert policy.delay(0) == 0.5
    assert policy.delay(1) == 1.0
    assert policy.delay(2) == 2.0
    assert policy.delay(3) == 4.0
    assert policy.delay(4) == 4.0  # 封顶
    assert policy.delay(10) == 4.0
