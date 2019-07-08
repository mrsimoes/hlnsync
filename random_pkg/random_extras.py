#!/usr/bin/env python

"""
Some functions for sampling useful random distributions.
"""

from six import integer_types

import sys
import random
import math


def geometric_rv(p):
    """
    Return a sample of a rv with geometric distribution, parameter p,
    the number of Bernoulli trials to get the first success.
    p - the probability of success.
    Expected value: 1/p.
    """
    if p == 1:
        return 1
    assert p != 0
    eps = sys.float_info.epsilon
    u = random.random()
    val = math.ceil(math.log(u, 1-p))
    assert val >= 1
    return val

def binomial_rv(p, n):
    """
    Return a sample of a rv with binomial distribution of parameters p, n.
    (This is the number of successes in N Bernoulli trials, each with prob.
    p of succeeding.)
    The result is in the range [0,n], including endpoints.
    The expected value is p times n.
    """
    assert 0 <= p <= 1
    if p > .5:
        return n - binomial_rv(1-p, n)
    elif p == 0:
        return 0
    else:
        suc_cnt = 0
        cur_trial = 0
        while True:
            cur_trial += geometric_rv(p) # 1 <= geometric_rv(p)
            if cur_trial <= n:
                suc_cnt += 1
            else:
                break
        return suc_cnt

def randint_avg(avg):
    """
    Return a random non-negative integer, with given expected value avg,
    using a geometric variable.
    """
    val = geometric_rv(1.0/avg) - 1
    assert val >= 0
    return int(val)

def randint_intv_avg(a, b, avg):
    """
    Return a random integer on range [a, b], including endpoints,
    with expected value avg, using a binomial distribution.
    """
    assert a <= avg <= b
    n = b - a
    val = a + binomial_rv(avg/n, n)
    assert a <= val <= b
    return int(val)

def dirichlet_vec_rv(n, T=1.0):
    """
    Return a list of n equally distributed non-negative real random numbers
    x1...xn, with sum T.
    Compute the first n-1 according to appropriate Dirichlet distributions
    D(T,n) with cdf F_t,n = 1-(1-x/T)^n on [0, T].
    X[n-1] is D(n-1,T), X[n-2] is D(n-2,T-X[n-1]), down to X[1] is D(1,T-(X[2]+...+X[n-1]))
    and X[0] is the remainder.
    """
    assert T >= 0 and isinstance(n, integer_types)
    result = [0]*n
    sum_computed = 0
    for k in range(n-1, 0, -1):
        Tk = T - sum_computed
        result[k] = Tk * (1.0 - (random.random())**(1.0/k))
        sum_computed += result[k]
    result[0] = T - sum_computed
    return result

def dirichlet_vec_discrete_rv(n, T):
    """
    Return a list of equally distributed n random non-negative integer values
    x1,...,xn summing to T, a positive integer.
    """
    assert isinstance(n, integer_types) and isinstance(T, integer_types) and T >= 0
    res = [int(xi) for xi in dirichlet_vec_rv(n, T)]
    while sum(res) < T:
        p = random.randint(0, n-1)
        if res[p] < T:
            res[p] += 1
    return res
    