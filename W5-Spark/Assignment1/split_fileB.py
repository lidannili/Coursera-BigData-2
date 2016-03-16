def split_fileB(line):
    # split the input line into word, date and count_string
    key_value = line.split(",")
    date_word = key_value[0].split(" ")
    word = date_word[1]
    date = date_word[0]
    count_string = key_value[1]
    return (word, date + " " + count_string) 

test_line = "Feb-22 actor,3"
split_fileB(test_line)


