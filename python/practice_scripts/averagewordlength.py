def AverageWordLength(sentence):
    if len(sentence) == 0:
        return 0

    words = sentence.split()
    word_length = []
    for word in range(len(words)):
        word_length.append(len(words[word]))
    print(word_length)
    return sum(word_length) / len(word_length)
