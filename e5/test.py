import numpy as np
import pandas as pd
import sys
from scipy import stats
from datetime import date
import matplotlib.pyplot as plt

OUTPUT_TEMPLATE = (
    "Initial T-test p-value: {initial_ttest_p:.3g}\n"
    "Original data normality p-values: {initial_weekday_normality_p:.3g} {initial_weekend_normality_p:.3g}\n"
    "Original data equal-variance p-value: {initial_levene_p:.3g}\n"
    "Transformed data normality p-values: {transformed_weekday_normality_p:.3g} {transformed_weekend_normality_p:.3g}\n"
    "Transformed data equal-variance p-value: {transformed_levene_p:.3g}\n"
    "Weekly data normality p-values: {weekly_weekday_normality_p:.3g} {weekly_weekend_normality_p:.3g}\n"
    "Weekly data equal-variance p-value: {weekly_levene_p:.3g}\n"
    "Weekly T-test p-value: {weekly_ttest_p:.3g}\n"
    "Mann-Whitney U-test p-value: {utest_p:.3g}"
)

filename1 = sys.argv[1]

counts = pd.read_json(filename1, lines=True)
counts['day'] = counts['date'].apply(date.weekday)
counts['year'] = counts['date'].dt.year
counts = counts[(counts['year'] == 2013) | (counts['year'] == 2012)]
counts = counts[(counts['subreddit'] == "canada")]

counts.loc[counts['day'] < 5, 'isWeekday'] = 'True'
counts.loc[counts['day'] >= 5, 'isWeekday'] = 'False'
weekday = counts.loc[counts['isWeekday'] == 'True']
weekend = counts.loc[counts['isWeekday'] == 'False']
print(weekday)

ttest = stats.ttest_ind(weekday['comment_count'], weekend['comment_count']).pvalue
init_pvalue_day = (stats.normaltest(weekday['comment_count']).pvalue)
init_pvalue_end = (stats.normaltest(weekend['comment_count']).pvalue)
init_equalvar = (stats.levene(weekday['comment_count'], weekend['comment_count']).pvalue)


weekday['test'] = np.sqrt(weekday['comment_count'])
weekend['test'] = np.sqrt(weekend['comment_count'])
trans_pvalue_end = (stats.normaltest(weekend['test']).pvalue)
trans_pvalue_day = (stats.normaltest(weekday['test']).pvalue)

trans_equalvar = (stats.levene(weekday['test'], weekend['test']).pvalue)

def calender(dateObj):
    return dateObj.isocalendar()[:2]

weekend['year_week'] = weekend['date'].apply(calender)
weekend2 = weekend.groupby(by=['year_week'])['comment_count'].mean()
weekday['year_week'] = weekday['date'].apply(calender)
weekday2 = weekday.groupby(by=['year_week'])['comment_count'].mean()

weekly_day = (stats.normaltest(weekday2).pvalue)
weekly_end = (stats.normaltest(weekend2).pvalue)
weekly_equalvar = (stats.levene(weekday2, weekend2).pvalue)
weekly_ttest = stats.ttest_ind(weekday2, weekend2).pvalue

utest = (stats.mannwhitneyu(weekday['comment_count'], weekend['comment_count'], alternative='two-sided').pvalue)

def main():
    reddit_counts = sys.argv[1]

    # ...

    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p= ttest,
        initial_weekday_normality_p=init_pvalue_day,
        initial_weekend_normality_p=init_pvalue_end,
        initial_levene_p=init_equalvar,
        transformed_weekday_normality_p=trans_pvalue_day,
        transformed_weekend_normality_p=trans_pvalue_end,
        transformed_levene_p=trans_equalvar,
        weekly_weekday_normality_p=weekly_day,
        weekly_weekend_normality_p=weekly_end,
        weekly_levene_p=weekly_equalvar,
        weekly_ttest_p=weekly_ttest,
        utest_p= utest,
    ))


if __name__ == '__main__':
    main()