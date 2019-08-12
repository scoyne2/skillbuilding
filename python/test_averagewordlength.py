from averagewordlength import AverageWordLength


def test_average_word_length():
    assert AverageWordLength("this is ok test") == 3
    assert AverageWordLength("") == 0
    assert AverageWordLength("supercalifragilisticexpialidocious") == 34
