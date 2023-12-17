import ping3
import psutil
import socket
import time
from confluent_kafka import Producer
import datetime




# Thiết lập cấu hình Kafka
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Địa chỉ Kafka broker
    'client.id': 'ip_status_producer'
}

# Tạo một producer Kafka
producer = Producer(kafka_conf)

def get_all_ip_addresses():
    ip_addresses = []
    # Lấy danh sách tất cả các mạng dang hoat dong trên máy tính
    network_interfaces = psutil.net_if_addrs()

    for interface, addrs in network_interfaces.items():
        for addr in addrs:
            if addr.family == socket.AF_INET:
                ip_addresses.append((interface, addr.address))   
    return ip_addresses

def check_ip_status(interface, ip_address):
    try:
        #thoi gian hien tai
        current_time = datetime.datetime.now()
        #Lấy thời gian phản hồi
        response_time = ping3.ping(ip_address, timeout=2)
        #Nếu phản hồi là None thì offline
        if response_time is not None:
            message = f"{interface}, {ip_address}, {current_time}, online"
            #print(f" {interface}, IP {ip_address} dang hoat dong. Thoi gian phan hoi: {response_time} ms")
        else:
            message = f"{interface}, {ip_address}, {current_time}, offline"
            #print(f" {interface}, Khong nhan duoc phan hoi tu IP {ip_address}")
        return message.encode("utf-8")
    except Exception as e:
        message = f"Error: {e}"
        #print(f"Loi khi kiem tra IP {ip_address} tren {interface}: {e}")
        return message.encode("utf-8")
#lấy địa chỉ ip các thiết bị mạng
ip_addresses = get_all_ip_addresses()

def send_ip_status_to_kafka(ip_status_info):
    topic = 'ip_status_data'  # Tên chủ đề Kafka
    key = 'ip_status_info'  # Key của thông điệp

    # Gửi thông tin vào Kafka
    producer.produce(topic, key=key, value=ip_status_info)
    producer.flush()

#vòng True gửi thông tin trạng thái mạng liên tục
while True:
    for interface, ip_address in ip_addresses:
        ip_status_info = check_ip_status(interface, ip_address)
        send_ip_status_to_kafka(ip_status_info)
        time.sleep(3) #ngủ 2s giữa mỗi lần gửi thông số
    #ngủ 3s giữa các lần lặp lại kiểm tra trạng thái mạng
    time.sleep(5)