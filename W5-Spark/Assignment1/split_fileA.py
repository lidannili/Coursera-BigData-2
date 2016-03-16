def split_fileA(line):
    # split the input line in word and count on the comma
    key_value  = line.split(",")   #split line, into key and value, returns a list
    word     = key_value[0]  #key is first item in list
    count   = int(key_value[1])   ## turn the count to an integer
    return (word, count)

test_line = "able,991"
split_fileA(test_line)
