/** ======================˵��=================================================**/


/**** �����б�:
(1) eval_pfmance


**/


/** =======================================================================**/



/***  ģ��0: �����������ּ�¼������end_date,effective_date **/
/** ȫ�ֱ�: 
(1) busday **/
/** ����: 
(1) index_pool: ���Ƚ���ϣ�����:date/&index_ret. (��������)
(2) bm_pool: ��׼��ϣ�����:date/&bm_ret. (��������)
(3) index_ret: ���Ƚ����������������
(4) bm_ret: ��׼���������������
(5) start_date: �Ƚ����俪ʼ��
(6) end_date: �Ƚ����������
(7) type: 1- ֻ����alpha / 2- ֻ����index_pool�еľ�������
(8) output_table: year/ret/std/ir/hit_ratio/index_draw (yea=0��ʾ�����������ڣ����ݾ����껯)

ע������������index_pool��bm_pool���ǵķ�Χ��ͬ��start_date��end_date���Զ���������֤���ڶ��ߵĸ���ȥ���������ڼ���alphaʱ���׳���
***/
/**** (ȫ����) ***/
/** ʤ��Ϊ�¶�ʤ�� */
%MACRO eval_pfmance(index_pool, bm_pool, index_ret, bm_ret, start_date, end_date, type, output_table);
	%IF %SYSEVALF(&type. = 1) %THEN %DO;
		PROC SQL;
			CREATE TABLE tt_summary_day AS
			SELECT A.date, coalesce(A.&index_ret.,0)-coalesce(B.&bm_ret.,0) AS ret
			FROM &index_pool. A LEFT JOIN &bm_pool. B
			ON A.date = B.date 
			WHERE A.date >= (SELECT min(date) FROM &bm_pool.) 
			AND A.date <= (SELECT max(date) FROM &bm_pool.)
			AND A.date >= "&start_date."d
			AND A.date <= "&end_date."d
			ORDER BY A.date;
		QUIT;
	%END;
	%ELSE %IF %SYSEVALF(&type.= 2) %THEN %DO;
		PROC SQL;
			CREATE TABLE tt_summary_day AS
			SELECT A.date, coalesce(A.&index_ret.,0) AS ret
			FROM &index_pool. A
			WHERE A.date >= "&start_date."d
			AND A.date <= "&end_date."d
			ORDER BY A.date;
		QUIT;
	%END;
	/** ����ָ�� */
	DATA tt_summary_day;
		SET tt_summary_day;
		RETAIN accum_ret 0;
		accum_ret = ((accum_ret/100 + 1)*(1+ret/100)-1)*100; /* �Ը�Ȩ���Ӽ��� */
		index = 1000 * (1+accum_ret/100);
		year = year(date);
		month = month(date);
	RUN;

	/* Step1: �����ͳ�� */
	/* Step1-1:����ȼ������س����ۼ�����(δ�껯) */
	DATA tt_summary_day(drop = max_index);
		SET tt_summary_day;
		BY year;
		RETAIN max_index .;
		RETAIN accum_ret_year 0;
		IF first.year THEN DO;
			max_index = .;
			accum_ret_year = 0;
		END;
		IF index >= max_index THEN max_index = index;
		index_draw_year = (index - max_index)/max_index *100;
		accum_ret_year  = ((1+accum_ret_year/100)*(1+ret/100)-1)*100;
	RUN;

	DATA tt_stat1(rename = (accum_ret_year = accum_ret));
		SET tt_summary_day(keep = year accum_ret_year) ;
		BY year;
		IF last.year;
	RUN;
	/* Step1-2: ����ȣ�������+ ������ + IR + ʤ�� + ���س�(%) */
	PROC SQL;
		CREATE TABLE tt_stat2 AS
		SELECT year, 
		sqrt(var(ret))*sqrt(250) AS sd,
		sum(ret)*sqrt(250)/(count(1)*sqrt(var(ret))) AS ir,
		sum(ret>0)/count(1) AS hit_ratio,
		min(index_draw_year) AS index_draw
		FROM tt_summary_day
		GROUP BY year;
	QUIT;
	/** Step1-3: ����ȣ��¶�ʤ�� */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT year, month, sum(ret) AS m_ret
		FROM tt_summary_day
		GROUP BY year, month;
	QUIT;
	PROC SQL;
		CREATE TABLE tt_stat3 AS
		SELECT year, sum(m_ret>0)/count(1) AS hit_ratio_m
		FROM tmp
		GROUP BY year;
	QUIT;

	PROC SQL;
		CREATE TABLE tt_summary_stat1 AS
		SELECT A.*, B.*, C.hit_ratio_m
		FROM tt_stat1 A JOIN tt_stat2 B
		ON A.year = B.year
		JOIN tt_stat3 C
		ON A.year = C.year
		ORDER BY A.year;
	QUIT;
	

	/* Step2: �����ز��������� */
	/* Step2-1: ������+ ������ + IR + ʤ�� + ���س�(%) */
	DATA tt_summary_day(drop = max_index);
		SET tt_summary_day;
		BY year;
		RETAIN max_index .;
		IF index >= max_index THEN max_index = index;
		index_draw = (index - max_index)/max_index *100;
	RUN;

	/** Step2-2: ����ȣ��¶�ʤ�� */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT year, month, sum(ret) AS m_ret
		FROM tt_summary_day
		GROUP BY year, month;
	QUIT;
	PROC SQL;
		CREATE TABLE tt_stat3 AS
		SELECT 0 AS year, sum(m_ret>0)/count(1) AS hit_ratio_m
		FROM tmp
	QUIT;

	PROC SQL;
		CREATE TABLE tt_summary_stat2 AS
		SELECT A.*, B.hit_ratio_m 
		FROM
		(SELECT 0 AS year,
		mean(ret)*250 AS accum_ret,
		sqrt(var(ret))*sqrt(250) AS sd,
		sum(ret)*sqrt(250)/(count(1)*sqrt(var(ret))) AS ir,
		sum(ret>0)/count(1) AS hit_ratio,
		min(index_draw) AS index_draw
		FROM tt_summary_day) A LEFT JOIN tt_stat3 B
		ON A.year = B.year;
	QUIT;

	DATA &output_table.;
		SET tt_summary_stat2 tt_summary_stat1;
	RUN;
	PROC SQL;
		DROP TABLE tmp, tt_summary_stat1, tt_summary_stat2, tt_stat1, tt_stat2, tt_stat3, tt_summary_day;
	QUIT;
%MEND eval_pfmance;
