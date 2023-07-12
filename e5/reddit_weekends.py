import sys
import pandas as pd
from scipy import stats
import numpy as np


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


def main():
    counts = pd.read_json(sys.argv[1], lines=True)
    counts = counts[counts['subreddit'] == 'canada']
    counts = counts[(counts['date'].dt.year == 2012) | (counts['date'].dt.year == 2013)]

    weekday_counts = counts[(counts['date'].dt.weekday !=5) & (counts['date'].dt.weekday !=6)]
    weekend_counts = counts[(counts['date'].dt.weekday ==5) | (counts['date'].dt.weekday ==6)]
    initial_ttest_p = stats.ttest_ind(weekend_counts['comment_count'],weekday_counts['comment_count']).pvalue
    initial_weekday_normality_p = stats.normaltest(weekday_counts['comment_count']).pvalue
    initial_weekend_normality_p = stats.normaltest(weekend_counts['comment_count']).pvalue
    initial_levene_p = stats.levene(weekend_counts['comment_count'], weekday_counts['comment_count']).pvalue

    #FIX 1
    sqrt_weekday = np.sqrt(weekday_counts['comment_count'])
    sqrt_weekend = np.sqrt(weekend_counts['comment_count'])
    transformed_weekday_normality_p = stats.normaltest(sqrt_weekday).pvalue
    transformed_weekend_normality_p = stats.normaltest(sqrt_weekend).pvalue
    transformed_levene_p = stats.levene(sqrt_weekend,sqrt_weekday).pvalue
    
    #FIX 2
    separate_weekdays = weekday_counts['date'].dt.isocalendar()
    separate_weekdays = separate_weekdays[['year','week']]
    weekday_counts = pd.concat([weekday_counts, separate_weekdays], axis=1)
    separate_weekends = weekend_counts['date'].dt.isocalendar()
    separate_weekends = separate_weekends[['year','week']]
    weekend_counts = pd.concat([weekend_counts, separate_weekends], axis=1)

    weekday_counts_CLT = weekday_counts.groupby(['year', 'week']).mean()
    weekend_counts_CLT = weekend_counts.groupby(['year', 'week']).mean()

    weekly_weekday_normality_p = stats.normaltest(weekday_counts_CLT['comment_count']).pvalue
    weekly_weekend_normality_p = stats.normaltest(weekend_counts_CLT['comment_count']).pvalue
    weekly_levene_p = stats.levene(weekday_counts_CLT['comment_count'],weekend_counts_CLT['comment_count']).pvalue  
    weekly_ttest_p = stats.ttest_ind(weekday_counts_CLT['comment_count'],weekend_counts_CLT['comment_count']).pvalue

    #FIX 3
    utest_p = stats.mannwhitneyu(weekday_counts['comment_count'],weekend_counts['comment_count'], use_continuity=True, alternative='two-sided').pvalue

    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p = initial_ttest_p,
        initial_weekday_normality_p = initial_weekday_normality_p,
        initial_weekend_normality_p = initial_weekend_normality_p,
        initial_levene_p = initial_levene_p,
        transformed_weekday_normality_p = transformed_weekday_normality_p,
        transformed_weekend_normality_p = transformed_weekend_normality_p,
        transformed_levene_p = transformed_levene_p,
        weekly_weekday_normality_p = weekly_weekday_normality_p,
        weekly_weekend_normality_p = weekly_weekend_normality_p,
        weekly_levene_p = weekly_levene_p,
        weekly_ttest_p = weekly_ttest_p,
        utest_p = utest_p,
    ))


if __name__ == '__main__':
    main()
