from confluent_kafka import Consumer, KafkaError
import pandas as pd
# Thiết lập cấu hình Kafka
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Địa chỉ Kafka broker
    'group.id': 'ip_status_group',  # Tên của nhóm consumer
    'auto.offset.reset': 'earliest'  # Đặt lại vị trí offset ban đầu
}

# Tạo một consumer Kafka
consumer = Consumer(kafka_conf)

# Đăng ký chủ đề cần đọc
topic = 'ip_status_data'  # Tên chủ đề Kafka
consumer.subscribe([topic])
# Tạo DataFrame rỗng
df = pd.DataFrame(columns=["Name", "IP Address", "Timestamp", "Status"])
#Thêm header vào file csv
df.to_csv("data-20120466.csv",  header=True, index=False)
while True:
    msg = consumer.poll(1.0)  # Lấy thông điệp từ Kafka

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(f'Lỗi Kafka: {msg.error().str()}')
            break

    # Hiển thị thông điệp ip_status #.decode("utf-8")
    print(f'Nhận thông điệp ip_status: {msg.value().decode("utf-8")}')
    
    #Đổi từ bytes sang string
    data = msg.value().decode("utf-8")

    # Tách dữ liệu bằng dấu ,
    data_list = data.split(", ")  
    if len(data_list) == 4:
        name, ip_address, timestamp, status = data_list
        # Tạo DataFrame tạm thời từ dữ liệu hiện tại
        temp_df = pd.DataFrame({"Name": [name], "IP Address": [ip_address], "Timestamp": [timestamp], "Status": [status]})
        #lưu vào file csv
        #mode a để lưu vào cuối tệp
        temp_df.to_csv("data-20120466.csv", mode='a', header=False, index=False)
        # Sử dụng concat để kết hợp DataFrame tạm thời vào DataFrame chính
        df = pd.concat([df, temp_df], ignore_index=True)

#print(df)

consumer.close()
