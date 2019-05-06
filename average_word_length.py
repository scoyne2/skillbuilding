def average_word_length(sentence):
    if len(sentence) == 0:
        return 0

    words = sentence.split()
    word_length = []
    for word in range(len(words)):
        word_length.append(len(words[word]))
    print(word_length)
    return sum(word_length) / len(word_length)


def test_average_word_length():
    assert average_word_length("this is ok test") == 3
    assert average_word_length("") == 0
    assert average_word_length("supercalifragilisticexpialidocious") == 34
