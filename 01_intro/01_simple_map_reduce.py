from mrjob.job import MRJob

class MRWordCount(MRJob):

    def mapper(self, _, line):
        yield 'chars', len(line)
        yield 'words', len(line.split())

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordCount.run()

# Definiujemy job jako klasę, która dzidziczy po klasie MRJob
# Ta klasa zawiera metody, które pozwalają definiować poszczególne kroki joba
# Jeden krok składa się z trzech etapów:
# - mapper
# - combiner
# - reducer
# Wszystkie trzy są opcjonalne, natomiast musimy przekazać co najmniej jeden

# Aby uruchomić joba należy użyć komendy:
# $ python [nazwa_skryptu.py] [nazwa_pliku_wejściowego.txt]
# $ python 01_simple_map_reduce.py sample01.txt

# Można także przekazać więcej plików wejściowych
# $ python  01_simple_map_reduce.py sample01.txt sample02.txt

# Domyślnie mrjob uruchomi naszego joba jako jeden proces. Daje to łatwą możliwość debugowana.
# natomiast na tym etapie pomijamy zalety przetwarzania rozproszonego

# Uruchomienie joba na klastrze chmurowym (Elastic MapReduce):
# - konfiguracja awscli
# - wykonanie polecenia z flagą -r/--runner emr, np.
# $ python 01_count words_job.py -r emr s3://bucket-name/data.txt