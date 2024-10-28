import nltk
nltk.download("stopwords")

import fileproc

# пути до файлов
task_file = 'files/volgait2024-semifinal-task.csv'
addresses_file = 'files/volgait2024-semifinal-addresses.csv'
output_file = 'files/volgait2024-semifinal-result.csv'

fileproc.identify_queries_csv(task_file)
fileproc.search_addresses(addresses_file, output_file)
