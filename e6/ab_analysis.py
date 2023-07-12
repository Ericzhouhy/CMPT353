import sys
import pandas as pd
import numpy as np
from scipy.stats import chi2_contingency
from scipy.stats import mannwhitneyu



OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value:  {more_users_p:.3g}\n'
    '"Did users search more/less?" p-value:  {more_searches_p:.3g} \n'
    '"Did more/less instructors use the search feature?" p-value:  {more_instr_p:.3g}\n'
    '"Did instructors search more/less?" p-value:  {more_instr_searches_p:.3g}'
)


def main():
    searchdata_file = sys.argv[1]
    data = pd.read_json(searchdata_file,orient='records',lines=True)
    even = data[data['uid']%2 == 0].reset_index().drop(['index'],axis = 1)
    odd = data[data['uid']%2 != 0].reset_index().drop(['index'], axis=1)

    even_not_search = even[even['search_count'] == 0]
    even_search = even[even['search_count'] !=0 ]
    odd_not_search = odd[odd['search_count'] == 0]
    odd_search = odd[odd['search_count'] != 0]

    even_not_search_count = len(even_not_search)
    even_search_count = len(even_search)
    odd_not_search_count = len(odd_not_search)
    odd_search_count = len(odd_search)

    obs =  np.array([[even_search_count,even_not_search_count],
            [odd_search_count,odd_not_search_count]])

    stat, more_users_p, dof, expected = chi2_contingency(obs)
    more_searches_p = mannwhitneyu(even['search_count'],odd['search_count']).pvalue

    instr_even = even[even['is_instructor'] == True].reset_index().drop(['index'],axis = 1)
    instr_odd = odd[odd['is_instructor'] == True].reset_index().drop(['index'],axis = 1)

    instr_even_not_search = instr_even[instr_even['search_count'] == 0]
    instr_even_search = instr_even[instr_even['search_count'] !=0 ]
    instr_odd_not_search = instr_odd[instr_odd['search_count'] == 0]
    instr_odd_search = instr_odd[instr_odd['search_count'] != 0]

    instr_even_not_search_count = len(instr_even_not_search)
    instr_even_search_count = len(instr_even_search)
    instr_odd_not_search_count = len(instr_odd_not_search)
    instr_odd_search_count = len(instr_odd_search)

    instr_obs =  np.array([[instr_even_search_count,instr_even_not_search_count],
            [instr_odd_search_count,instr_odd_not_search_count]])

    instr_stat, more_instr_p, instr_dof, instr_expected = chi2_contingency(instr_obs)
    more_instr_searches_p = mannwhitneyu(instr_even['search_count'],instr_odd['search_count']).pvalue
    
    print(OUTPUT_TEMPLATE.format(
        more_users_p=more_users_p,
        more_searches_p=more_searches_p,
        more_instr_p=more_instr_p,
        more_instr_searches_p=more_instr_searches_p,
    ))


if __name__ == '__main__':
    main()
