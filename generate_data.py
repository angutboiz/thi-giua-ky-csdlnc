import sys
import random
import pandas as pd
from datetime import datetime, timedelta

def generate_transactions(student_id, num_records=1000000):
    transactions = []
    
    start_date = datetime.now() - timedelta(days=730)  
    
    for i in range(num_records):
        transaction_id = f"TXN_{i:07d}"
        customer_id = f"STD_{student_id}"
        order_date = start_date + timedelta(days=random.randint(0, 730))
        price = round(random.uniform(10, 1000), 2)
        quantity = random.randint(1, 10)
        discount = round(random.uniform(0, 0.5), 2)
        
        if random.random() < 0.1:
            if random.random() < 0.5:
                price = "INVALID"
            else:
                quantity = "NaN"
        
        transactions.append([transaction_id, customer_id, order_date, price, quantity, discount])
    
    return transactions

def main():
    student_id = 172100123
    filename = f"transactions_{student_id}.csv"
    
    data = generate_transactions(student_id)
    df = pd.DataFrame(data, columns=["transaction_id", "customer_id", "order_date", "price", "quantity", "discount"])
    
    df.to_csv(filename, index=False)
    print(f"Generated file: {filename}")

if __name__ == "__main__":
    main()