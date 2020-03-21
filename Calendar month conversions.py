

#Creating a month name to month number mapping (eg. Jan - 1, Feb - 2 ...etc)

months=dict((v,k) for k,v in enumerate(calendar.month_abbr))
month_df=pd.DataFrame({'Calendar_Month_Name':list(months.keys()),'Calendar_Month':list(months.values())})

#month number to name
months=dict((k,v) for k,v in enumerate(calendar.month_abbr))
def prev_ym(y,m,n=3):
    if m-n<=0:
        m=m-n+12
        y=y-1
    else:m=m-n
    Cal_YM=y*100+m
    Cal_YMS=str(y)+"-"+str(months[m])
    return Cal_YM , Cal_YMS