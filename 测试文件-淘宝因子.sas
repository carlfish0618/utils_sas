%LET taobao_dir = D:\Research\������;
%LET input_dir = &taobao_dir.\input_data; 
%LET output_dir = &taobao_dir.\output_data;
LIBNAME taobao "&taobao_dir.\sasdata";

options validvarname=any; /* ֧�����ı����� */
/***************************** PART I: ����׼�� *********************/
/** ���:
(1) taobao.month_list: ����ÿ�������һ�������գ�����֮��1-12���·� 
(2) taobao.factors_value: ��ҵ���ӣ���ҵ�ڸ������ӵ�ֵ��������ͨ��ֵ��Ȩ)��ԭʼ�÷� 
(3) taobao.indus_close: ��ҵ��λ(��Ȩ)����ʱ�������������£��������� 
(4) taobao.stock_pool: ������������ĳɷֹɣ�����ӳ�� **/

/** ��ע:
	(1) ��ҵ���ӵ÷��ɷ��F�ṩ
	(2) ��ҵ��λ���ĺ��ṩ
	(3) ��������������2014-12-22�汾(ed3) */ 


/**Step1: �������ӵ÷� */


/** �۳���ҵ���� */
%LET fname = updnrec_gg;

PROC SQL;
	CREATE TABLE indus_score AS
	SELECT end_date, A.indus_code, B.indus_name, sum(&fname.*fmv_sqr)/sum(fmv_sqr*(not missing(&fname.)* (not missing(fmv_sqr)))) AS &fname.,
		sum(&fname.*fmv_sqr)/sum(fmv_sqr) AS &fname._m,  
		sum(not missing(&fname.)* (not missing(fmv_sqr))) / count(1) AS pct, 
		count(1) AS nobs, sum(not missing(&fname.)* (not missing(fmv_sqr))) AS valid_nobs
	FROM fgtest.fg_taobao_st_score_ent A LEFT JOIN fgtest.fg_taobao_indus B
	ON A.indus_code = B.indus_code 
	GROUP BY end_date, A.indus_code
	ORDER BY pct;
QUIT;
/*DATA tt;*/
/*	SET indus_score;*/
/*	IF indus_name = "��������";*/
/*RUN;*/
/*PROC SORT DATA = tt;*/
/*	BY end_date;*/
/*RUN;*/

PROC SQL;
	CREATE TABLE indus_score AS
	SELECT end_date, 
		sum(not missing(&fname.)* (not missing(fmv_sqr))) / count(1) AS pct, 
		count(1) AS nobs, sum(not missing(&fname.)* (not missing(fmv_sqr))) AS valid_nobs
	FROM fgtest.fg_taobao_st_score_ent
	GROUP BY end_date
	ORDER BY end_date;
QUIT;





/* Step1: ������������ */
PROC IMPORT OUT = taobao.factors_value
            DATAFILE= "&input_dir.\factors_value_ed6.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="��ҵ�¶ȵ÷�$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=NO;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
/** Step1-appendix: �����Ա��������� */
PROC IMPORT OUT = factors_value
            DATAFILE= "&input_dir.\taobao_factors.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="��ҵ�¶ȵ÷�$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=NO;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
PROC TRANSPOSE DATA = factors_value OUT = factors_value(drop = _LABEL_ rename=(_NAME_ = fg_level2_name col1 = taobao));
	VAR Ӱ�Ӷ���--������Ƶ;
	BY end_date;
RUN;
PROC SORT DATA = taobao.indus_value;
	BY end_date fg_level2_name;
RUN;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.taobao
	FROM taobao.factors_value A LEFT JOIN factors_value B
	ON A.end_date = B.end_date AND A.fg_level2_name = B.fg_level2_name
	ORDER BY A.end_date, A.fg_level2_name;
QUIT;
DATA taobao.factors_value;
	SET tmp;
	zh1 = 0.5*tot2 + 0.5*taobao;
	zh2 = 0.75*tot2 + 0.25*taobao;
	zh3 = 0.25*tot2 + 0.75*taobao;
RUN;


/** Step2: ������ҵ������ */
PROC IMPORT OUT = taobao.indus_close
            DATAFILE= "&input_dir.\indus_close_ed3.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="��ҵ����$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=NO;
     USEDATE=YES;
     SCANTIME=YES;
RUN;
PROC TRANSPOSE DATA = taobao.indus_close OUT = taobao.indus_close(drop = _LABEL_ rename=(_NAME_ = fg_level2_name col1 = close));
	VAR ����--��������;
	BY end_date;
RUN;
PROC SORT DATA = taobao.indus_close;
	BY end_date fg_level2_name;
RUN;


/** Step3: ����ɷֹ�(��������ӳ���ϵ) */
PROC IMPORT OUT = taobao.stock_pool
            DATAFILE= "&input_dir.\entertainment_ed3.xlsx" 
            DBMS=EXCEL REPLACE;
     RANGE="��Ʊ���������ƥ��$"; 
     GETNAMES=YES;
     MIXED=NO;
     SCANTEXT=NO;
     USEDATE=YES;
     SCANTIME=YES;
RUN;

/**  Step3: �������ڱ� **/

/* ȡÿ�������һ�������� */
 PROC SQL;
	CREATE TABLE busday AS
	SELECT DISTINCT end_date AS date
	FROM taobao.hqinfo
	ORDER BY date;
QUIT;

DATA busday;
	SET busday;
	IF not missing(date);
	date = datepart(date);
	month = month(date);
	year = year(date);
	FORMAT date mmddyy10.;
RUN;
PROC SORT DATA = busday;
	BY year month date;
RUN;
DATA busday_month;
	SET busday;
	BY year month;
	IF last.month;
RUN;
DATA busday_month;
	SET busday_month;
	id = _N_;
RUN;

/** ����δ��1-12���� **/
/** ����taobao.month_list **/
%MACRO month_forward();
	DATA ini_t;
		SET busday_month(keep = date id);
	RUN;
	%DO i = 1 %TO 12;
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, B.date AS date_&i. 
			FROM ini_t A LEFT JOIN busday_month B
			ON B.id = A.id + &i.
			ORDER BY A.id;
		QUIT;
		DATA ini_t;
			SET tmp;
		RUN;
	%END;
	DATA taobao.month_list;
		SET ini_t;
	RUN;
	PROC SQL;
		DROP TABLE ini_t, tmp, busday_month;
	QUIT;
%MEND;

%month_forward();

/********************* PART II: ��֤������Ч�� **/
/** �����
(1) taobao.return_table: ����ÿ����ҵ�ۼ�1-12���µ������� 
(2) taobao.fname_stat: ÿ������һ����Ӧ��񣬰���spearman_ic, pearson_ic, �Լ���Ч��������
(3) taobao.ic_mean: ������������ʱ��ic�ľ�ֵ
(4) taobao.ic_hitratio: ������������ʱ��ic�У�ic>0�ı���


/* Step1: ����������ҵ���ۻ������� */
%MACRO cal_accum_return();
	/** ȡ��ҵ����ĩ�����̵�λ */
	PROC SQL;
		CREATE TABLE indus_close_sub AS
		SELECT end_date As date LABEL "date", fg_level2_name, close 
		FROM taobao.indus_close
		WHERE end_date IN
		(SELECT date FROM taobao.month_list)
		ORDER BY fg_level2_name, date;
	QUIT;

	/* ѭ������δ��1-12���µ��ۼ������� */
	%DO index = 1 %TO 12;
		PROC SQL;
			CREATE TABLE tt_accum AS
			SELECT A.date, A.fg_level2_name, A.close, B.date_&index. LABEL "date_&index.", C.close AS close_&index. LABEL "close_&index."
			FROM indus_close_sub A LEFT JOIN taobao.month_list B
			ON A.date = B.date
			LEFT JOIN 
			(SELECT date, fg_level2_name, close FROM indus_close_sub) C
			ON A.fg_level2_name = C.fg_level2_name AND B.date_&index = C.date
			ORDER BY A.fg_level2_name, A.date;
		QUIT;
		
		/* �����ۼ������� */
		DATA tt_accum(drop = date_&index. close_&index.);
			SET tt_accum;
			IF not missing(close) AND not missing(close_&index.) THEN DO;
				accum_&index. = (close_&index.-close)/close * 100;
			END;
			ELSE accum_&index. = .;
		RUN;
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, B.accum_&index.
			FROM indus_close_sub A LEFT JOIN tt_accum B
			ON A.fg_level2_name = B.fg_level2_name AND A.date = B.date;
		QUIT;
		DATA indus_close_sub;
			SET tmp;
		RUN;
		PROC SQL;
			DROP TABLE tt_accum, tmp;
		QUIT;
	%END;

	/*** ���ۼ������ʵ���Ϊ�������µ�������(Ϊ�˱�����Ĵ��룬��������Ϊaccum_*����ʽ */
	DATA taobao.return_table(drop = i single_2-single_12);
		SET indus_close_sub;
		ARRAY accum_var(12) accum_1-accum_12;
		ARRAY single_var(11) single_2-single_12;
		DO i = 1 TO 11;
			IF not missing(accum_var(i+1)) AND not missing(accum_var(i)) THEN 
				single_var(i) = ((1+accum_var(i+1)/100)/(1+accum_var(i)/100)-1)*100;
			ELSE single_var(i) = .;
		END;
		ARRAY accum_var2(11) accum_2-accum_12;
		ARRAY single_var2(11) single_2-single_12;
		DO i = 1 TO 11;
			accum_var2(i) = single_var2(i);
		END;
	RUN;

	PROC SQL;
		DROP TABLE indus_close_sub;
	QUIT;
%MEND;
%cal_accum_return();


PROC SQL;
	CREATE TABLE tt AS
	SELECT *
	FROM top_alpha
	WHERE factor_name = "CRATSALE_JT"
	ORDER BY sample_ret desc;
QUIT;

	
/** Step2: ���ÿ�����ӷֱ����IC */
/*** �ڽ����ϣ������⼸����ҵ�������ʺ�����֮������ϵ��(pearson��spearman) */

/** ÿ����������: fname_t(��������ֵ���ۼ�������), fname_stat(ͳ��ic����Ч����������) **/
%LET start_date = 31dec2009;
%LET ending_date = 31oct2014;

%MACRO single_factor_ic(fname);
	DATA &fname._t;
		SET taobao.factors_value(keep = end_date fg_level2_name &fname. rename = (end_date = date));
		IF not missing(&fname.);
		FORMAT date mmddyy10.;
	RUN;
	PROC SQL;
		CREATE TABLE &fname._stat AS
		SELECT distinct date 
		FROM &fname._t
		WHERE "&start_date."d <= date <= "&ending_date."d
		ORDER BY date;
	QUIT;


	/* ѭ�����δ��1-12���µ��ۼƻ��������� */
	%DO index = 1 %TO 12;
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, B.accum_&index.
			FROM &fname._t A LEFT JOIN taobao.return_table B
			ON A.fg_level2_name = B.fg_level2_name AND A.date = B.date
			ORDER BY A.date, A.fg_level2_name;
		QUIT;

		/* �������ϵ����ȥ��ȱʧֵ */
		/** �޳�����������/��������/������װ���� */
		DATA &fname._t;
			SET tmp;
			IF fg_level2_name IN ("��������", "��������", "������װ") THEN delete;
			IF not missing(accum_&index.) AND not missing(&fname.);
		RUN;
		PROC CORR DATA = &fname._t pearson spearman OUTP = corr_p OUTS = corr_s NOPRINT;
			BY date;
			VAR &fname. accum_&index.;
		RUN;
		/** pearson */
		DATA corr_p(keep = date _TYPE_ accum_&index.);
			SET corr_p;
			IF upcase(_NAME_) = upcase("&fname.") OR _TYPE_ = "N";
		RUN;
		PROC TRANSPOSE DATA = corr_p OUT = corr_p(keep = date corr N rename = (N = nobs_&index. corr = p_ic_&index.));
			BY date;
			VAR accum_&index.;
			ID  _TYPE_;
		RUN;
		/* spearman */
		DATA corr_s(keep = date _TYPE_ accum_&index.);
			SET corr_s;
			IF upcase(_NAME_) = upcase("&fname.") OR _TYPE_ = "N";
		RUN;
		PROC TRANSPOSE DATA = corr_s OUT = corr_s(keep = date corr N rename = (N = nobs_&index. corr = s_ic_&index.));
			BY date;
			VAR accum_&index.;
			ID  _TYPE_;
		RUN;

		/* ���л��� */
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, B.nobs_&index., B.p_ic_&index., C.s_ic_&index.
			FROM  &fname._stat A 
			LEFT JOIN corr_p B
			ON A.date = B.date
			LEFT JOIN corr_s C
			ON A.date = C.date
			ORDER BY A.date;
		QUIT;
		DATA &fname._stat;
			SET tmp;
		RUN;

		PROC SQL;
			DROP TABLE tmp, corr_p, corr_s;
		QUIT;
	%END;

	/* ���浽�̶���library��*/
	DATA taobao.&fname._stat;
		SET &fname._stat;
	RUN;
	PROC SQL;
		DROP TABLE &fname._t, &fname._stat;
	QUIT;
%MEND single_factor_ic;

/*%single_factor_ic(fname = crateps_gg);*/


/** ѭ����ÿ�����ӷֱ����ic **/
/** �����б�: factor_list **/

%MACRO loop_factor_ic();
	/** ���������б� */
	PROC CONTENTS DATA = taobao.factors_value OUT = taobao.factor_list(keep = name) NOPRINT;
	RUN;
	DATA taobao.factor_list;
		SET taobao.factor_list;
		IF upcase(name) not IN ("END_DATE", "FG_LEVEL2_NAME");
	RUN;
	PROC SQL NOPRINT;
		SELECT name, count(1) 
		INTO :name_list SEPARATED BY ' ',
			 :nfactors
		FROM taobao.factor_list;
	QUIT;
			
	%DO i = 1 %TO &nfactors.;
		%LET fname =  %scan(&name_list.,&i., ' ');
		%single_factor_ic(fname = &fname.);
	%END;
%MEND;

%loop_factor_ic();


/**  Step3: ͳ����������ic�ľ�ֵ/ʤ�� **/
/** ���ɱ�: ic_mean, ic_hitratio **/
%MACRO all_ic_analyze();

	%LET flag = %SYSFUNC(exist(taobao.ic_mean));
	%IF %SYSEVALF(&flag. = 1) %THEN %DO;
		PROC SQL;
			DROP TABLE taobao.ic_mean;
		QUIT;
	%END;
	%LET flag = %SYSFUNC(exist(taobao.ic_hitratio));
	%IF %SYSEVALF(&flag. = 1) %THEN %DO;
		PROC SQL;
			DROP TABLE taobao.ic_hitratio;
		QUIT;
	%END;

	PROC SQL NOPRINT;
		SELECT name, count(1) 
		INTO :name_list SEPARATED BY ' ',
			 :nfactors
		FROM taobao.factor_list;
	QUIT;

	%DO i = 1 %TO &nfactors.;
		%LET fname =  %scan(&name_list.,&i., ' ');
		/* ��ֵ */
		PROC MEANS DATA = taobao.&fname._stat  NOPRINT;
			VAR p_ic_1-p_ic_12 s_ic_1-s_ic_12;
			OUTPUT OUT = tt;
		RUN;
		DATA tt(drop = _TYPE_ _FREQ_ _STAT_);
			LENGTH fname  $ 16.;
			fname = "&fname.";
			SET tt;
			IF _STAT_ = "MEAN";
		RUN;
		/** ʤ�� **/
		DATA tt_hit(drop = j);
			SET taobao.&fname._stat;
			ARRAY p_ic_var(12) p_ic_1-p_ic_12; 
			ARRAY p_hit_var(12) p_hit_1-p_hit_12;
			ARRAY s_ic_var(12) s_ic_1-s_ic_12;
			ARRAY s_hit_var(12) s_hit_1-s_hit_12;
			DO j = 1 TO 12;
				IF p_ic_var(j) > 0 THEN p_hit_var(j) = 1;
				ELSE IF not missing(p_ic_var(j)) THEN p_hit_var(j) = 0;
				ELSE p_hit_var(j) = .;

				IF s_ic_var(j) > 0 THEN s_hit_var(j) = 1;
				ELSE IF not missing(s_ic_var(j)) THEN s_hit_var(j) = 0;
				ELSE s_hit_var(j) = .;
			END;
		RUN;
		PROC MEANS DATA = tt_hit NOPRINT;
			VAR s_hit_1-s_hit_12 p_hit_1-p_hit_12;
			OUTPUT OUT = tt2;
		RUN;
		DATA tt2(drop = _TYPE_ _FREQ_ _STAT_);
			LENGTH fname  $ 16.;
			fname = "&fname.";
			SET tt2;
			IF _STAT_ = "MEAN";
		RUN;

		%IF %SYSEVALF(&i. = 1) %THEN %DO;
			DATA taobao.ic_mean;
				SET tt;
			RUN;
			DATA taobao.ic_hitratio;
				SET tt2;
			RUN;

		%END;
		%ELSE %DO;
			DATA taobao.ic_mean;
				SET taobao.ic_mean tt;
			RUN;
			DATA taobao.ic_hitratio;
				SET taobao.ic_hitratio tt2;
			RUN;
		%END;
		PROC SQL;
			DROP TABLE tt, tt2, tt_hit;
		QUIT;
	%END;
%MEND;

%all_ic_analyze();


PROC SQL;
	CREATE TABLE tt AS
	SELECT date, fg_level2_name, accum_1
	FROM taobao.return_table
	ORDER BY date, fg_level2_name;
QUIT;
PROC TRANSPOSE DATA = tt OUT = tt2 ;
	BY date;
	IDLABEL fg_level2_name;
	VAR accum_1;
RUN;
PROC SQL;
	SELECT distinct fg_level2_name
	FROM taobao.return_table
	ORDER BY fg_level2_name;
QUIT;
