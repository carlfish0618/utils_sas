/** ======================说明=================================================**/
/*** 函数功能：提供与日期无关的辅助的通用函数 **** /

/**** 函数列表:
(1) cal_coef: 计算pearson和spearman相关系数
(2) granger_test: 格兰杰检验
(3) test_stationarity: 稳定性检验
****/ 

/** =======================================================================**/


options validvarname=any; /* 支持中文变量名 */


/** 模块1: 计算pearson和spearman相关系数 */
%MACRO cal_coef(data, var1, var2, output_s = corr_s, output_p = corr_p);
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
