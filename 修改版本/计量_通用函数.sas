/** ======================说明=================================================**/
/*** 函数功能：提供与日期无关的辅助的通用函数 **** /

/**** 函数列表:
(1) cal_coef: 计算pearson和spearman相关系数
(2) granger_test: 格兰杰检验
(3) test_stationarity: 稳定性检验
(4) test_single_var_corr：检验单变量跨期间的相关性
(5) filter_outlier: 剔除异常值 (需要用到：因子计算_通用函数中的cal_percentile)
****/ 

/** =======================================================================**/

/** 模块1: 计算pearson和spearman相关系数 */
%MACRO cal_coef(data, var1, var2, output_s = corr_s, output_p = corr_p);
	PROC SORT DATA = &data.;
		BY end_date;
	RUN;
	PROC CORR DATA = &data. pearson spearman OUTP = corr_p OUTS = corr_s NOPRINT;
		WHERE not missing(&var1.) AND not missing(&var2.);
		BY end_date;
		VAR &var1. &var2.;
	RUN;
	DATA corr_p(keep = end_date _TYPE_ &var1.);
		SET corr_p;
		IF upcase(_NAME_) = upcase("&var2.") OR _TYPE_ = "N";
	RUN;
	PROC TRANSPOSE DATA = corr_p OUT = corr_p(keep = end_date corr N rename = (N = nobs corr = p_ic));
		BY end_date;
		VAR &var1.;
		ID  _TYPE_;
	RUN;
	DATA corr_s(keep = end_date _TYPE_ &var1.);
		SET corr_s;
		IF upcase(_NAME_) = upcase("&var2.") OR _TYPE_ = "N";
	RUN;
	PROC TRANSPOSE DATA = corr_s OUT = corr_s(keep = end_date corr N rename = (N = nobs corr = s_ic));
		BY end_date;
		VAR &var1.;
		ID  _TYPE_;
	RUN;
	DATA &output_s;
		SET corr_s;
	RUN;
	DATA &output_p;
		SET corr_p;
	RUN;
%MEND cal_coef;

/** 模块2：Granger Test */
/** 待修改：把lag弄成可以调整的 */
%MACRO granger_test(dataset, var1, var2, lag=1);
	DATA causal;
		SET &dataset.;
		&var1._1 = lag(&var1.);
		&var1._2 = lag2(&var1.);
		&var2._1 = lag(&var2.);
		&var2._2 = lag2(&var2.);
	RUN;

		 
	/* unrestricted model */
/*	PROC AUTOREG DATA = causal ;*/
/*		MODEL &var1. = &var1._1 &var1._2 &var2._1 &var2._2;*/
/*		OUTPUT OUT = out1 R = e1;  */
/*	RUN;*/

	PROC AUTOREG DATA = causal OUTEST = test_results ;
		MODEL &var1. = &var1._1 &var2._1;
		OUTPUT OUT = out1 R = e1;  
	RUN;

   /* restricted model */
/*   PROC AUTOREG DATA = causal;*/
/*		MODEL &var1. = &var1._1 &var1._2;*/
/*		OUTPUT OUT = out2 R = e0;*/
/*   RUN;*/

    PROC AUTOREG DATA = causal;
		MODEL &var1. = &var1._1;
		OUTPUT OUT = out2 R = e0;
   RUN;
   /** calculate f-test */
   ODS SELECT IML._LIT1010
              IML.TEST1_P_VAL1
              IML.TEST2_P_VAL2;

   ODS HTML body='exgran01.htm';           
   PROC IML;
      START MAIN;
      use out1;
      read all into e1 var{e1};
      close out1;

      use out2;
      read all into e0 var{e0};
      close out2;

      p = 1;           /* # of lags         */
      T = nrow(e1);    /* # of observations */
      sse1 = ssq(e1);
      sse0 = ssq(e0);
      * F test;
      test1 = ((sse0 - sse1)/p)/(sse1/(T - 2*p - 1));
      p_val1 = 1 - probf(test1,p,T - 2*p - 1);

      * asymtotically equivalent test;
      test2 = (T * (sse0 - sse1))/sse1;
      p_val2 = 1 - probchi(test2,p);

      print "IML Result",, test1 p_val1,,
                           test2 p_val2;
      finish;
   RUN;
   QUIT;
   ods html close;
%MEND granger_test;

/** 模块3: 检验平稳性 */
%MACRO test_stationarity(dataset, var, lag = 3);
	proc arima data=&dataset.;
   		identify var=&var. stationarity=(adf=&lag.) ;
		identify var=&var. stationarity=(pp=&lag.);
	quit;
%MEND test_stationarity;


/** 模块4: 检验单变量跨期间的相关性 */
/** 需要调用"其他_通用函数"中的： get_nearby_data 以及同个文件中的：cal_coef */
/**
(1) identity: 主体
(2) raw_col: 变量名
(3) date_col: 日期字段
(4) type: 1- spearman 2- pearson 3-nobs
(5) output_table
(6) offset_range: 期数
**/
%MACRO test_single_var_corr(input_table, identity, raw_col, date_col, 
		type, output_table, offset_range=12);
	DATA tt_single;
		SET &input_table.;
		KEEP &date_col. &identity. &raw_col.;
	RUN;
	PROC SQL;
		CREATE TABLE tt_output_s AS
		SELECT distinct &date_col.
		FROM tt_single
		ORDER BY &date_col.;
	QUIT;
	PROC SQL;
		CREATE TABLE tt_output_p AS
		SELECT distinct &date_col.
		FROM tt_single
		ORDER BY &date_col.;
	QUIT;
	PROC SQL;
		CREATE TABLE tt_output_nobs AS
		SELECT distinct &date_col.
		FROM tt_single
		ORDER BY &date_col.;
	QUIT;

	/* 生成未来12个间隔(3年)的因子 */
	%DO offset = 1 %TO &offset_range.;
		%get_nearby_data(input_table=tt_single, identity=&identity., raw_col = &raw_col., 
			date_col= &date_col., output_col = &raw_col._&offset., 
			offset = &offset., output_table=tt_single);
		%cal_coef(tt_single, &raw_col., &raw_col._&offset., output_s = corr_s, output_p = corr_p);
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, B.s_ic AS s_ic_&offset.
			FROM tt_output_s A LEFT JOIN corr_s B
			ON A.&date_col. = B.&date_col.
			ORDER BY A.&date_col.;

			CREATE TABLE tmp2 AS
			SELECT A.*, B.p_ic AS p_ic_&offset.
			FROM tt_output_p A LEFT JOIN corr_p B
			ON A.&date_col. = B.&date_col.
			ORDER BY A.&date_col.;

			CREATE TABLE tmp3 AS
			SELECT A.*, B.nobs AS nobs_&offset.
			FROM tt_output_nobs A LEFT JOIN corr_p B
			ON A.&date_col. = B.&date_col.
			ORDER BY A.&date_col.;
		QUIT;
		DATA tt_output_s;
			SET tmp;
		RUN;
		DATA tt_output_p;
			SET tmp2;
		RUN;
		DATA tt_output_nobs;
			SEt tmp3;
		RUN;
		%IF %SYSEVALF(&type.=1) %THEN %DO;
			DATA &output_table.;
				SET tt_output_s;
			RUN;
		%END;
		%ELSE %IF %SYSEVALF(&type.=2) %THEN %DO;
			DATA &output_table.;
				SET tt_output_p;
			RUN;
		%END;
		%ELSE %IF %SYSEVALF(&type.=3) %THEN %DO;
			DATA &output_table.;
				SET tt_output_nobs;
			RUN;
		%END;

		PROC SQL;
			DROP TABLe corr_s, corr_p, tmp, tmp2, tmp3;
		QUIT;
	%END;
	PROC SQL;
		DROP TABLE tt_output_p, tt_output_s, tt_output_nobs;
	QUIT;

%MEND test_single_var_corr;


/*** 模块5：剔除异常值，仅保留特定区间的取值 **/
%MACRO filter_outlier(input_table, colname, output_table, upper=99, lower=1, group_var=end_date, is_filter=1);
	%cal_percentitle(&input_table., &colname., &lower., pct&lower., group_var=&group_var.);
	%cal_percentitle(&input_table., &colname., &upper., pct&upper., group_var=&group_var.);
	PROC SQL;
		CREATE TABLE tt_raw AS
		SELECT A.*, B.pct&lower., C.pct&upper.
		FROM &input_table. A LEFT JOIN pct&lower. B
		ON A.&group_var. = B.&group_var.
		LEFT JOIN pct&upper. C
		ON A.&group_var. = C.&group_var.
		ORDER BY A.&group_var.;
	QUIT;
	%IF %SYSEVALF(&is_filter.=0) %THEN %DO;   
		/* winsorize */
		DATA &output_table.;
			SET tt_raw;
			IF not missing(&colname.) AND &colname. > pct&upper. THEN &colname. = pct&upper.;
			ELSE IF not missing(&colname.) AND &colname. < pct&lower. THEN &colname. = pct&lower.;
		RUN;
	%END;
	%ELSE %DO;   /* 替换初始值 */
		DATA &output_table.;
			SET tt_raw;
			IF not missing(&colname.) AND &colname. > pct&upper. THEN delete;
			ELSE IF not missing(&colname.) AND &colname. < pct&lower. THEN delete;
		RUN;
	%END;
	PROC SQL;
		DROP TABLE tt_raw, pct&lower., pct&upper.;
	QUIT;
%MEND filter_outlier;
