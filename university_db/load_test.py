import time
import random
import string
import threading
from pymongo import MongoClient
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "university_db"
COLLECTION = "students"

FACULTIES = ["Информатика", "Математика", "Физика", "Экономика", "Юриспруденция"]
COURSES_LIST = ["Алгоритмы", "БД", "ОС", "Матан", "Физика", "Экономика", "Право", "ML"]

results = {"insert": [], "read": []}
lock = threading.Lock()


def random_student(i: int) -> dict:
    return {
        "student_id": i,
        "name": "Студент_" + "".join(random.choices(string.ascii_uppercase, k=5)),
        "faculty": random.choice(FACULTIES),
        "year": random.randint(1, 5),
        "gpa": round(random.uniform(2.5, 5.0), 2),
        "courses": random.sample(COURSES_LIST, k=random.randint(2, 5)),
    }


def benchmark_inserts(col, n: int, batch_size: int = 500) -> float:
    """Вставка n документов батчами, возвращает ops/sec."""
    docs = [random_student(i) for i in range(n)]
    start = time.perf_counter()
    for i in range(0, n, batch_size):
        col.insert_many(docs[i:i + batch_size], ordered=False)
    elapsed = time.perf_counter() - start
    return n / elapsed


def benchmark_reads(col, n: int) -> float:
    """n случайных чтений по student_id, возвращает ops/sec."""
    # Берём реальные ID из коллекции
    sample_ids = [d["student_id"] for d in col.aggregate([{"$sample": {"size": min(n, 1000)}}])]
    if not sample_ids:
        return 0.0
    start = time.perf_counter()
    for _ in range(n):
        sid = random.choice(sample_ids)
        _ = col.find_one({"student_id": sid})
    elapsed = time.perf_counter() - start
    return n / elapsed


def run_benchmark():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    col = client[DB_NAME][COLLECTION]

    # Чистим коллекцию перед тестом
    col.drop()
    client[DB_NAME].command(
        "shardCollection", f"{DB_NAME}.{COLLECTION}",
        key={"student_id": "hashed"}
    )

    print("=" * 55)
    print("  Нагрузочное тестирование MongoDB Sharded Cluster")
    print("=" * 55)

    volumes = [500, 1000, 2000, 5000, 10000]
    insert_ops = []
    read_ops = []

    for n in volumes:
        col.drop()
        # Небольшой прогрев
        col.insert_many([random_student(i) for i in range(100)], ordered=False)

        ins = benchmark_inserts(col, n)
        insert_ops.append(ins)
        print(f"  INSERT  n={n:>6}  →  {ins:>8.0f} ops/sec")

        rd = benchmark_reads(col, min(n, 2000))
        read_ops.append(rd)
        print(f"  READ    n={n:>6}  →  {rd:>8.0f} ops/sec")
        print()

    client.close()

    # --- Визуализация ---
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("Нагрузочное тестирование MongoDB Sharded Cluster (2 шарда)", fontsize=13)

    axes[0].plot(volumes, insert_ops, marker="o", color="#e74c3c", linewidth=2)
    axes[0].set_title("Пропускная способность INSERT")
    axes[0].set_xlabel("Количество документов")
    axes[0].set_ylabel("ops/sec")
    axes[0].grid(True, alpha=0.3)
    axes[0].fill_between(volumes, insert_ops, alpha=0.1, color="#e74c3c")

    axes[1].plot(volumes, read_ops, marker="s", color="#2980b9", linewidth=2)
    axes[1].set_title("Пропускная способность READ (find_one)")
    axes[1].set_xlabel("Количество операций")
    axes[1].set_ylabel("ops/sec")
    axes[1].grid(True, alpha=0.3)
    axes[1].fill_between(volumes, read_ops, alpha=0.1, color="#2980b9")

    plt.tight_layout()
    plt.savefig("load_test_results.png", dpi=150)
    print("✓ График сохранён: load_test_results.png")

    # --- Распределение по шардам ---
    client2 = MongoClient(MONGO_URI)
    col2 = client2[DB_NAME][COLLECTION]
    stats = client2[DB_NAME].command("collStats", COLLECTION)
    shards = stats.get("shards", {})
    if shards:
        print("\n--- Распределение по шардам ---")
        for shard_name, shard_data in shards.items():
            print(f"  {shard_name}: {shard_data.get('count', 0)} документов")
    client2.close()

    print("\n=== Тестирование завершено ===")


if __name__ == "__main__":
    run_benchmark()
