import csv
import random
import string
from datetime import datetime, timedelta

def random_string(length=6):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

def generate_transaction(student_id, index):
    customer_id = f"STD_{student_id}"
    transaction_id = f"TXN_{random.randint(100000, 999999)}"
    order_date = datetime.now() - timedelta(days=random.randint(0, 365), seconds=random.randint(0, 86400))
    price = round(random.uniform(10, 2000), 2)
    quantity = random.randint(1, 10)
    discount = round(random.uniform(0, 0.5), 2)  # ví dụ: 0.15 là 15% giảm giá
    location = random.choice(["Hanoi", "Ho Chi Minh", "Da Nang", "Can Tho", "Hai Phong"])
    return [transaction_id, customer_id, order_date.isoformat(), price, quantity, discount, location]

def generate_malformed_transaction():
    # Tạo dòng lỗi với trường thiếu hoặc sai định dạng
    options = [
        lambda: ["MALFORMED_LINE_NO_ID"],  # thiếu cột
        lambda: ["TXN_ERR", "STD_XYZ", "not_a_date", "??", "x", "??", "Unknown"],  # sai định dạng
        lambda: ["", "", "", "", "", "", ""],  # dòng rỗng
        lambda: ["TXN_123", None, "bad_time", 1000, 3, 0.1, "Nowhere"],  # sai định dạng thời gian
    ]
    return random.choice(options)()

def main():
    student_id = 172100123
    filename = f"transactions_{student_id}.csv"

    total_rows = 1_000_000
    malformed_count = int(total_rows * 0.10)

    print(f"Generating {total_rows} transactions...")
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["transaction_id", "customer_id", "order_date", "price", "quantity", "discount", "location"])

        for i in range(total_rows):
            if i < malformed_count:
                row = generate_malformed_transaction()
            else:
                row = generate_transaction(student_id, i)
            writer.writerow(row)

    print(f"✅ File '{filename}' generated successfully.")

if __name__ == "__main__":
    main()
