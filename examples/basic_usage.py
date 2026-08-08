import redis
from bloomsieve import BloomFilter, BloomFilterService


def main():
    # 1. Simple Standalone Bloom Filter (Local In-Memory)
    print("--- 1. Standalone Bloom Filter ---")
    bf = BloomFilter(capacity=1000, error_rate=0.01)
    
    # Add an item
    bf.add("hello")
    
    # Check membership using standard Python 'in' operator
    print(f"Contains 'hello'? {'hello' in bf}")  # True
    print(f"Contains 'world'? {'world' in bf}")  # False

    # 2. Simple Redis-Backed Bloom Filter Service
    print("\n--- 2. Redis-Backed Bloom Filter Service ---")
    try:
        # Create a standard Redis connection
        client = redis.StrictRedis(host="localhost", port=6379, db=0, socket_timeout=1)
        client.ping()
        
        # Initialize service wrapper
        service = BloomFilterService(redis_client=client, capacity=1000, error_rate=0.01)
        filter_name = "simple_demo"
        
        # Reserve the filter on Redis
        service.createFilter(filter_name)
        
        # Add and check items
        service.add(filter_name, "hello")
        print(f"Exists 'hello' in Redis? {service.exists(filter_name, 'hello')}")  # True
        print(f"Exists 'world' in Redis? {service.exists(filter_name, 'world')}")  # False
        
        # Clean up Redis key
        client.delete(filter_name)
    except redis.ConnectionError:
        print("Skipping Redis demo: Local Redis server is not running on localhost:6379.")


if __name__ == "__main__":
    main()
