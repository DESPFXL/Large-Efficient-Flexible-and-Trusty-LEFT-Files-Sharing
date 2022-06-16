import os
from socket import *
from os.path import join
from threading import Thread
import time
import struct
import argparse
import socket
import math


def _argparse():
    """
    Input the peer ip
    :return:
    """
    parse = argparse.ArgumentParser(description="IP")
    parse.add_argument('--ip', type=str, help='IP')
    return parse.parse_args()


IP = _argparse().ip
II_port = 21414
file_port = 22414
folder_port = 24414
run_port = 27414
recover_list = []
file_list_g = []
main_dir = 'share'
div = '/'
mtime_table = {}
prefix = '192'


def getmtime(path):
    return os.stat(path).st_mtime


def find_if_online():
    """
    Figure out whether peer machine is online or not
    :return:
    """
    test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        test_socket.connect((IP, run_port))
        test_socket.close()
        return 0
    except:
        return -1


def ready_for_run():
    """
    Bind with the run port, then the port can run the 'find_online' method
    :return:
    """
    online_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    online_socket.bind(('', run_port))
    online_socket.listen(3)
    while True:
        _, _ = online_socket.accept()


def make_header(interactive_information, filename, size, position):
    """
    Make the header
    :param interactive_information: interactive_information is the operation code to the
    :param filename: the file name
    :param size: the file size
    :param position: the existed file size
    :return: header for send
    """
    if size > 0:
        size = struct.pack('!I', size)
    else:
        size = struct.pack('!I', 0)
    header = interactive_information + size + struct.pack('!I', position) + filename.encode()
    new_header = struct.pack('!I', len(header)) + header
    return new_header


def parse_header(socket1):
    """
    Parse the header
    :param socket1: a header socket
    :return: a list of unpacked header information
    """
    interactive_info_length = socket1.recv(4)
    length = struct.unpack('!I', interactive_info_length)[0]
    interactive_info1 = socket1.recv(length)
    interactive_info = struct.unpack('!I', interactive_info1[:4])[0]
    size = struct.unpack('!I', interactive_info1[4:8])[0]
    pos = struct.unpack('!I', interactive_info1[8:12])[0]
    filename = interactive_info1[12:].decode()
    header = [interactive_info, filename, size, pos]
    return header


def get_file_block(file_name, file_size, num_block, pos):
    """
    Get the block of a file
    :param file_name: file which should be blocked
    :param file_size: file size
    :param num_block: the start index of block
    :param pos: position for seek
    :return: the stored block
    """
    block_size = math.ceil(file_size / 33)
    f = open(join(main_dir, file_name), 'rb')
    f.seek(num_block * block_size + pos)
    block = f.read(block_size)
    f.close()
    return block


def judgement(interactive_info, filename, size, pos, send_socket):
    """
        The receive block has it own information:{
                                              0 --> send folder
                                              1 --> send file
                                              2 --> receive folder
                                              3 --> receive file
                                              4 --> resend or update
                                              }
    and the received file will be change to its original name with no prefix to show the send complete
    :param interactive_info:
    :param filename:
    :param size:
    :param pos:
    :param send_socket:
    :return:
    """
    new_name = os.path.join(main_dir, prefix + filename)
    old_name = os.path.join(main_dir, filename)
    if interactive_info == 4:
        pos = os.path.getsize(new_name) if os.path.exists(new_name) else 0
        if os.path.exists(old_name):
            os.remove(old_name)
        interactive_information = struct.pack('!I', 1)
        send_socket.connect((IP, II_port))
        send_socket.send(make_header(interactive_information, filename, size, pos))
        f = open(new_name, 'ab')
        receive_file(f, file_port, filename, '')
        print('Finish resending or updating')
    if interactive_info == 3:
        if filename not in file_list_g:
            file_list_g.append(filename)
            send_socket.connect((IP, II_port))
            interactive_information = struct.pack('!I', 1)
            info1 = make_header(interactive_information, filename, size, 0)
            send_socket.send(info1)
            f = open(new_name, 'wb')
            receive_file(f, file_port, filename, '')
            file_list_g.append(filename)
            send_socket.close()
    if interactive_info == 2:
        if filename not in file_list_g:
            file_list_g.append(filename)
            os.makedirs(new_name)
            send_socket.connect((IP, II_port))
            send_socket.send(make_header(struct.pack('!I', 0), filename, 0, 0))
            send_socket.close()
            receive_folder(filename)
            os.rename(new_name, old_name)
    if interactive_info == 0:
        time.sleep(0.01)
        send_folder(filename, 23565)
    if interactive_info == 1:
        print('start sending')
        time.sleep(0.01)
        try:
            sendfile(main_dir, filename, file_port, pos)
        except Exception as e:
            print(e)
            while True:
                if find_if_online() == 0:
                    resend(filename)
                    break


def sendfile(main_dir1, filename, port, pos):
    """
    Send file
    :param main_dir1:
    :param filename:
    :param port:
    :param pos:
    :return:
    """
    send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    send_socket.connect((IP, port))
    size = os.path.getsize(os.path.join(main_dir1, filename))
    if size >= 2048:
        for i in range(33):
            send_socket.send(get_file_block(filename, size, i, pos))
    else:
        f = open(os.path.join(main_dir1, filename), 'rb')
        send_socket.send(f.read())
        f.close()
    send_socket.close()


def send_folder(filename, port):
    """
    Send folder
    :param filename:
    :param port:
    :return:
    """
    folder = os.path.join(main_dir, filename)
    file_list = os.listdir(folder)
    send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    send_socket.connect((IP, folder_port))
    size = len(','.join(file_list).encode())
    interactive_info = struct.pack('!I', size)
    send_socket.send(interactive_info)
    send_socket.send(','.join(file_list).encode())
    for file in file_list:
        port += 1
        time.sleep(0.01)
        sendfile(folder, file, port, 0)


def receive_folder(filename):
    """
    Receive folder
    :param filename:
    :return:
    """
    folder_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    folder_socket.bind(('', folder_port))
    folder_socket.listen(3)
    folder_socket1, address = folder_socket.accept()
    length = struct.unpack('!I', folder_socket1.recv(4))[0]
    file_list = folder_socket1.recv(length).decode().split(',')
    folder = os.path.join(main_dir, prefix + filename)
    port = 23565
    for file in file_list:
        port = port + 1
        f = open(os.path.join(folder, prefix + file), 'wb')
        receive_file(f, port, file, filename)


def receive_file(f, port, filename, folder):
    """
    Receive file
    :param f:
    :param port:
    :param filename:
    :param folder:
    :return:
    """
    rev_file_socket_23 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rev_file_socket_23.bind(('', port))
    rev_file_socket_23.listen(20)
    receive_file_socket, _ = rev_file_socket_23.accept()
    while True:
        text = receive_file_socket.recv(20480)
        f.write(text)
        if text == b'':
            break
    f.close()
    print('Finish ', filename, '====================================')
    rev_file_socket_23.close()
    if prefix + filename not in mtime_table.keys():
        if folder == '':
            os.rename(os.path.join(main_dir, prefix + filename), os.path.join(main_dir, filename))
            mtime_table[filename] = getmtime(os.path.join(main_dir, filename))
        else:
            path = os.path.join(main_dir, prefix + folder)
            os.rename(os.path.join(path, prefix + filename), os.path.join(path, filename))


def find_new():
    """
    Traverse the 'share' folder once and once again, if find a new one,
    first communicate with the peer then decide the next step
    :return:
    """
    for file in os.listdir(main_dir):
        if not os.path.isdir(div.join((main_dir, file))) and file[:3] != prefix:
            mtime_table[file] = getmtime(os.path.join(main_dir, file))
    while True:
        time.sleep(0.01)
        current_list = os.listdir(main_dir)
        for f in mtime_table:
            if mtime_table[f] != getmtime(os.path.join(main_dir, f)):
                update(f)
                mtime_table[f] = getmtime(os.path.join(main_dir, f))
        if len(current_list) > len(file_list_g):
            for file in current_list:
                if file.startswith(prefix) and file[3:] not in file_list_g:
                    file_list_g.append(file[3:])
                elif file not in file_list_g:
                    if os.path.isdir(os.path.join(main_dir, file)):
                        time.sleep(2)
                    if find_if_online() == 0:
                        time.sleep(0.01)
                        interactive_change(file)
                        file_list_g.append(file)


def update(file):
    """
    Send header for machine when finding the updated file
    :param file:
    :return:
    """
    interactive_information = struct.pack('!I', 4)
    header = make_header(interactive_information, file, os.path.getsize(os.path.join(main_dir, file)), 0)
    update_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    update_socket.connect((IP, II_port))
    update_socket.send(header)
    update_socket.close()


def interactive_change(file):
    """
    Communicate with peer machine
    :param file: which file should be communicate with peer machine
    :return:
    """
    inter_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    inter_socket.connect((IP, II_port))
    if os.path.isdir(os.path.join(main_dir, file)):
        interactive_information = struct.pack('!I', 2)
    else:
        interactive_information = struct.pack('!I', 3)
    size = os.path.getsize(os.path.join(main_dir, file))
    header = make_header(interactive_information, file, size, 0)
    inter_socket.send(header)
    inter_socket.close()


def create_share(main_dir1):
    """
    Create a folder
    :param main_dir1:
    :return:
    """
    if not os.path.exists(main_dir1):
        os.makedirs(main_dir1)


def resend(filename):
    """
    Resend a file, in this coursework, for the 500MB file to resend the rest file content
    and if resend successfully, the 'resend_time' will add, and this method will not be called
    until the 'resend_time' changing to ''
    :param filename:
    :return:
    """
    interactive_information = struct.pack('!I', 4)
    header = make_header(interactive_information, filename, os.path.getsize(os.path.join(main_dir, filename)), 0)
    res_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    res_socket.connect((IP, II_port))
    res_socket.send(header)
    res_socket.close()


def receive():
    """
    The receive block has it own information:{
                                              0 --> send folder
                                              1 --> send file
                                              2 --> receive folder
                                              3 --> receive file
                                              4 --> resend or update
                                              }
    and the received file will be change to its original name with no prefix to show the send complete
    :return:
    """
    print('Receiving')
    rev_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    rev_socket.bind(('', II_port))
    rev_socket.listen(3)

    while True:
        send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        use_socket, _ = rev_socket.accept()
        info = parse_header(use_socket)
        interactive_info, filename, size, pos = info
        judgement(interactive_info, filename, size, pos, send_socket)


def main():
    create_share(main_dir)
    p1 = Thread(target=receive)
    p2 = Thread(target=ready_for_run)
    p3 = Thread(target=find_new)
    p1.start()
    p2.start()
    p3.start()
    p1.join()
    p2.join()
    p3.join()


if __name__ == '__main__':
    main()
