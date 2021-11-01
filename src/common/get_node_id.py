import hashlib


def get_node_id(user_id, accumulator_count):
    return str(int(hashlib.md5(user_id.encode('utf-8')).hexdigest(), 16) % accumulator_count)
