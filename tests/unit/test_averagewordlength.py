from python import averagewordlength as av


def test_average_word_length():
    assert av.AverageWordLength("this is ok test") == 3
    assert av.AverageWordLength("") == 0
    assert av.AverageWordLength("supercalifragilisticexpialidocious") == 34
