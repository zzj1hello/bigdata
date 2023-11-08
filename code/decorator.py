import time


def power_sum(n):
    return sum([i**2 for i in range(1, n+1)])

n = 1000000
start = time.time()
print(power_sum(n))
end = time.time()
print(end-start)

def add_func(target):

    def decorator(*args, **kwargs):

        start = time.time()
        res = target(*args, **kwargs)
        end = time.time()
        print(end-start)
        return res

    return decorator

print(add_func(power_sum)(n))

# 简化写法
@add_func
def power_sum_dec(n):
    return sum([i**2 for i in range(1, n+1)])

print(power_sum_dec(n))

@add_func
def power_sum_dec_3(n):
    return sum([i**3 for i in range(1, n+1)])
print(power_sum_dec_3(n))


# 额外套上参数
def add_2(precision):
    def add_func(target): # 只能放函数形参

        def decorator(*args, **kwargs):

            start = time.time()
            res = target(*args, **kwargs)
            end = time.time()
            print(round(end-start, precision))
            return res

        return decorator

    return add_func
precision = 2
# 带参数的装饰器
print(add_2(precision)(power_sum_dec)(n))

@add_2(precision)
def power_sum_dec_args(n):
    return sum([i**2 for i in range(1, n+1)])
print(power_sum_dec_args(n))

# 没法同时加入函数形参和普通参数
# def add_func_2(target, precision):

#     def decorator(*args, **kwargs):

#         start = time.time()
#         res = target(*args, **kwargs)
#         end = time.time()
#         print(round(end-start, precision))
#         return res

#     return decorator
# @add_func_2(target=power_sum_dec_args, precision=precision)
# def power_sum_dec_args_2(n):
#     return sum([i**2 for i in range(1, n+1)])
# print(power_sum_dec_args_2(n))
