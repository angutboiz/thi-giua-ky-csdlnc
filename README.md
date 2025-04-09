# Bước 1: chạy file generate_data.py

# Bước 2

-   Dùng PowerShell

-   Sử dụng hadoop nên cần cấu hình môi trường cho nó, Nhớ thay đường dẫn về file hadoop-3.0.0

```bash
$env:HADOOP_HOME = "hadoop-3.0.0"
$env:PATH += ";hadoop-3.0.0\bin"
```

-   Dùng CMD

```bash
set HADOOP_HOME=hadoop-3.0.0
set PATH=%PATH%;hadoop-3.0.0\bin
```

## Sau đó chạy process_transactions.py
