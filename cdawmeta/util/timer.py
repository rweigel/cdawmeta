import time

def tick():
  return time.perf_counter()

def tock(begin):
  return time.perf_counter() - begin
