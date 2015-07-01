%LET pfolio_env_start_date = 15dec2010;
%LET adjust_start_date = 15jan2011;
%LET adjust_end_date = 30jun2015;

%get_month_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., 
	rename=end_date, output_table=adjust_busdate, type=1);
%get_date_windows(raw_table=adjust_busdate, colname=end_date, output_table = adjust_busdate2, start_intval =-2, end_intval = 2);

PROC SQL;
	CREATE TABLE raw_table AS
	SELECT end_date, stock_code, close
	FROM hqinfo
	where end_date in 
	(SELECT end_date FROM adjust_busdate)
	AND index(stock_code,"600") = 1
	ORDER BY end_date, stock_code;
QUIT;

%cal_intval_return(raw_table=raw_table, group_name=stock_code, price_name=close, date_table=adjust_busdate2, output_table=ot2, is_single = 1);
DATA factor_table;
	SET raw_table(keep = end_date stock_code);
	test_f = _N_;
	test_f2 = _N_**2;
RUN;
/*%single_factor_ic(factor_table=factor_table, return_table=ot2, group_name=stock_code, fname=test_f);*/
%loop_factor_ic(factor_table=factor_table, return_table=ot2, group_name=stock_code);

