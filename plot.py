import pandas
import matplotlib.pyplot as plt

result = pandas.read_table('output/output.txt', header=1, sep="|")
result = result.iloc[1:-2, 1:-1]
result = result.rename(columns=lambda x: x.strip())
country_list = result.iloc[:,0].unique()
year_list = result.iloc[:,1].unique()
year_list.sort()
for i in year_list :
    x = country_list
    y = result[result['o_year'] == i]
    plt.subplot(4, 2, int(i) - 1991)
    plt.bar(country_list, y['sum_profit'])
    plt.xticks(fontsize=3)
    plt.xlabel('Countries')  # Add a label to the x-axis
    plt.ylabel('Profit')  # Add a label to the y-axis
    plt.title(f'Total profit of 25 countries in %s' %int(i))

plt.show()

