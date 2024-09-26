import re 

def parse_sql(sql):
    sql = sql.lower().replace("\n", "").replace("\t", "")
    match = re.match(r"[a-z\_\s\"\']*\((.*)\)", sql)

    cols_string = match.group(1)
    columns = [col_string.strip() for col_string in cols_string.split(",")]
    return columns


def parse_varint(byte_stream):
    """
    Parses a varint from the given byte stream.
    
    :param byte_stream: A byte-like object to read the varint from.
    :return: The parsed 64-bit two's complement integer.
    """
    result = 0
    
    for i in range(9):  # Varint can be up to 9 bytes long
        byte = byte_stream[i]
        result = result << 7
        result |= byte & 0x7F

        if byte & 0x80 == 0:
            break
    
    return i+1, result  # If all 9 bytes are processed, this is the final result
