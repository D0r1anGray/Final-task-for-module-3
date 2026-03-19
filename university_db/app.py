from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson import ObjectId
import sys


MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "university_db"
COLLECTION = "students"


def get_collection():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    db = client[DB_NAME]
    return db[COLLECTION]


def add_student(col):
    print("\n--- Добавление студента ---")
    student_id = input("ID студента (число): ").strip()
    name = input("ФИО: ").strip()
    faculty = input("Факультет: ").strip()
    year = input("Курс (1-5): ").strip()
    gpa = input("GPA (например, 4.5): ").strip()
    courses = input("Курсы (через запятую): ").strip().split(",")

    doc = {
        "student_id": int(student_id),
        "name": name,
        "faculty": faculty,
        "year": int(year),
        "gpa": float(gpa),
        "courses": [c.strip() for c in courses],
    }
    result = col.insert_one(doc)
    print(f"\n✓ Студент добавлен. _id: {result.inserted_id}")


def find_student(col):
    print("\n--- Поиск студента ---")
    query_type = input("Искать по: (1) ID  (2) ФИО  (3) Факультету → ").strip()

    if query_type == "1":
        sid = int(input("Введите student_id: ").strip())
        docs = list(col.find({"student_id": sid}))
    elif query_type == "2":
        name = input("Введите ФИО (или часть): ").strip()
        docs = list(col.find({"name": {"$regex": name, "$options": "i"}}))
    elif query_type == "3":
        faculty = input("Факультет: ").strip()
        docs = list(col.find({"faculty": {"$regex": faculty, "$options": "i"}}))
    else:
        print("Неверный ввод")
        return

    if not docs:
        print("Студенты не найдены.")
    for d in docs:
        print(f"\n  ID: {d['student_id']} | {d['name']} | {d['faculty']} | "
              f"Курс {d['year']} | GPA {d['gpa']}")
        print(f"  Курсы: {', '.join(d.get('courses', []))}")


def list_students(col):
    print("\n--- Все студенты ---")
    limit = input("Сколько показать (по умолчанию 20): ").strip()
    limit = int(limit) if limit.isdigit() else 20
    docs = list(col.find().limit(limit))
    if not docs:
        print("База данных пуста.")
    for d in docs:
        print(f"  [{d['student_id']}] {d['name']} — {d['faculty']}, курс {d['year']}, GPA {d['gpa']}")
    print(f"\nВсего в коллекции: {col.count_documents({})}")


def delete_student(col):
    print("\n--- Удаление студента ---")
    sid = int(input("Введите student_id для удаления: ").strip())
    result = col.delete_many({"student_id": sid})
    if result.deleted_count:
        print(f"✓ Удалено документов: {result.deleted_count}")
    else:
        print("Студент не найден.")


def shard_stats(col):
    """Показывает распределение данных по шардам."""
    client = MongoClient(MONGO_URI)
    db = client["university_db"]
    stats = db.command("collStats", COLLECTION)
    print("\n--- Статистика шардирования ---")
    shards = stats.get("shards", {})
    if shards:
        for shard_name, shard_data in shards.items():
            count = shard_data.get("count", 0)
            size = shard_data.get("size", 0)
            print(f"  {shard_name}: {count} документов, {size / 1024:.1f} KB")
    else:
        print("  Данные о шардах недоступны (возможно, данных ещё мало).")
    print(f"  Всего документов: {stats.get('count', 0)}")


def main():
    print("=" * 50)
    print("  База данных студентов университета")
    print("  MongoDB Sharded Cluster")
    print("=" * 50)

    try:
        col = get_collection()
        col.database.client.admin.command("ping")
    except ConnectionFailure:
        print("\n✗ Не удалось подключиться к MongoDB на localhost:27017")
        print("  Убедитесь, что кластер запущен (docker-compose up -d)")
        sys.exit(1)

    print("✓ Подключение к кластеру установлено\n")

    MENU = {
        "1": ("Добавить студента", add_student),
        "2": ("Найти студента", find_student),
        "3": ("Список студентов", list_students),
        "4": ("Удалить студента", delete_student),
        "5": ("Статистика шардов", shard_stats),
        "0": ("Выход", None),
    }

    while True:
        print("\nМеню:")
        for key, (label, _) in MENU.items():
            print(f"  {key}. {label}")
        choice = input("\nВыберите действие: ").strip()

        if choice == "0":
            print("До свидания!")
            break
        elif choice in MENU:
            _, fn = MENU[choice]
            try:
                fn(col)
            except Exception as e:
                print(f"Ошибка: {e}")
        else:
            print("Неверный ввод, попробуйте снова.")


if __name__ == "__main__":
    main()
