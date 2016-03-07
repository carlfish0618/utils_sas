/** ���������Ч�ԵĻ��������������ϣ�ͬʱ�������ӷ����е��������ͷ����������� */

/*** �����б�
(1) merge_multiple_factor_ic_result: ��������ic/coverage�Ľ��������Excel�鿴 
		Ҫ��������"������Ч��_ͨ�ú���/test_multiple_factor_ic"�����ִ�иú��� 
(2) test_single_factor_group_ret: �������ӵ÷ַ�ΪN�飬�����Ȩ��ϣ��Ա�������ϵĲ���
(3) test_single_cfactor_higher_ret: �����÷֣�ȡ������ߵ�min(N,half(nstock))���ֱ𹹽���Ȩ�͵÷ּ�Ȩ���
(4) test_single_dfactor_higher_ret: ��ɢ�÷֣�ȡ�ض��÷ֹ�Ʊ��������Ȩ���
(5) test_multiple_factor_group_ret: �������ͬʱ����(2)
(6) test_multiple_factor_higher_ret: �������ͬʱ����(3)����(4)
(7) merge_factor_3group_result: ��(5)�еĽ��(Ҫ���3��)���л��ܣ��Է���Excel�鿴
(8) merge_factor_higher_result: ��(6)�еĽ�����л��ܣ��Է���Excel�鿴



/*** ģ��1����������ic/coverage�Ľ��������Excel�鿴 */
/** ��Ҫ�����У�������Ч��_ͨ�ú���/test_multiple_factor_ic�����ִ�иú��� */
/** ���룺
(1) factor_table: ���ӱ������漰��������Ӧ����test_multiple_factor_ic���õ���factor_table���Ӽ���
(2) merge_var: ��Ҫ��test_multiple_factor_ic��������(&fname._ic����&fname._cover�г��ֵı���)���磺
			&fname._ic�е�s_ic_f1 ���� &fname._cover�е�pct
(3) suffix: cover����ic�е�һ��
(4) is_hit: 1-����ʤ��(>=0��ռ��) 0-�����ֵ
(5) output_table: ��������������
	(i) factor: ��������
	(ii) &merge_var.:ͳ�Ƶı���
(6) exclude_list: factor_table�в����ǵ��ֶΣ�Ҫ���д
**/

%MACRO merge_multiple_factor_ic_result(factor_table, merge_var, suffix, is_hit, output_table, exclude_list=(''));
	DATA &output_table.;
		ATTRIB
		factor LENGTH = $30.
		&merge_var. LENGTH = 8
		;
		STOP;
	RUN;
	DATA rr_result;
		SET &factor_table.;
	RUN;

	PROC CONTENTS DATA = rr_result OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
          PROC SQL;
			CREATE TABLE tmp AS
			SELECT "&fname." AS factor, 
			mean(&merge_var.) AS &merge_var._mean,
			sum(&merge_var.>0)/sum(not missing(&merge_var.)) AS &merge_var._hit
			FROM &fname._&suffix.
		QUIT;
		%IF %SYSEVALF (&is_hit.=0) %THEN %DO;
			DATA &output_table.;
				SET &output_table. tmp(rename = (&merge_var._mean = &merge_var.) drop =&merge_var._hit );
			RUN;
		%END;
		%ELSE %DO;
			DATA &output_table.;
				SET &output_table. tmp(rename = (&merge_var._hit = &merge_var.) drop =&merge_var._mean);
			RUN;
		%END;	
     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2;
	QUIT;

%MEND merge_multiple_factor_ic_result;


/** ģ��2: �������ӵ÷ַ�ΪN�飬�����Ȩ��ϣ��Ա�N����ϵĲ��� */
/** ���룺
(1) factor_table: ���ӵ÷��б�
(2) fname: ��������
(3) bm_index: ֻ�е�index_result_type=1ʱ����Ч��Ҫ��: end_date, daily_ret
(4) index_result_type: 1-������� 2-��������
(5) adjust_date: �������ڱ�(end_date)
(6) test_date: ������ڱ�(date)
(7) start_date: ��Ч������ʼ����
(8) end_date: ��Ч������������ (ע�⣺7��8һ�����test_date����ʼ�ͽ�������)
(9) annualized_factor: ����annualizedʱ��factor
(10) ngroup: �ֳɵ�N��
(11) is_cut: 1- ��N��(�ʺ���������) 0- �ʺ���ɢ����(ֱ������fname��ֵ)

**/
/** �����
&fname._g&rank._index
&fname._g&rank._stat
&fname._g&rank._trade
**/
%MACRO test_single_factor_group_ret(factor_table, fname, bm_index, index_result_type,
				adjust_date, test_date,
				start_date, end_date, 
				annualized_factor,
				ngroup =3, is_cut=1);
	DATA &factor_table._c(keep = end_date stock_code &fname.);
		SET &factor_table.;
		IF not missing(&fname.);
	RUN;
	/** �ֳ�N�� */
	%single_factor_score(raw_table=&factor_table._c, identity=stock_code, factor_name=&fname.,
		output_table=&factor_table._c, is_increase = 1, group_num = &ngroup.);
	%IF %SYSEVALF(&is_cut.=1) %THEN %DO;
		%LET test_var = &fname._score;
	%END;
	%ELSE %DO;
		%LET test_var = &fname.;
	%END; 

	PROC SQL NOPRINT;
		SELECT distinct &test_var., count(distinct &test_var.) 
		INTO :test_var_list SEPARATED BY ' ',
			 :ntest_var
		FROM &factor_table._c;
	QUIT;
	
	%DO index = 1 %TO &ntest_var.;
		%LET rank = %scan(&test_var_list., &index., ' ');
		PROC SQL;
			CREATE TABLE &factor_table._c2 AS
			SELECT end_date, stock_code, 1 AS weight
			FROM &factor_table._c
			WHERE &test_var. = &rank.;
		QUIT;
		%construct_index(test_pool=&factor_table._c2, adjust_date=&adjust_date., test_date=&test_date.,
			bm_index_table=&bm_index.,   
			output_index_table=&fname._g&rank._index, 
			output_stat_table=&fname._g&rank._stat, 
			output_trade_table=&fname._g&rank._trade, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&start_date., 
			end_date=&end_date., 
			index_result_type = &index_result_type.,
			is_output=0, 
			annualized_factor=&annualized_factor.);
	%END;
	PROC SQL;
		DROP TABLE &factor_table._c2, &factor_table._c;
	QUIT;
%MEND test_single_factor_group_ret;

/*** ģ��3�������÷֣�ȡ������ߵ�min(N,half(nstock))���ֱ𹹽���Ȩ�͵÷ּ�Ȩ��� */
/** ע�⣺��������Ҫ���ݵ÷ּ�Ȩ������ϣ�����Ҫ��÷�>0����������Щ��������ӣ����Թ����Ӧ��"������"��Ҫ�����ϵ÷�>0*/
/** ���룺
(1) factor_table: ���ӵ÷��б�
(2) fname: ��������
(3) bm_index: ֻ�е�index_result_type=1ʱ����Ч��Ҫ��: end_date, daily_ret
(4) index_result_type: 1-������� 2-��������
(5) adjust_date: �������ڱ�(end_date)
(6) test_date: ������ڱ�(date)
(7) start_date: ��Ч������ʼ����
(8) end_date: ��Ч������������ (ע�⣺7��8һ�����test_date����ʼ�ͽ�������)
(9) annualized_factor: ����annualizedʱ��factor
(10) nstock: ȡ���ٹ�Ʊ
**/
/** �����
&fname._&nstock._index_e/w
&fname._&nstock._stat_e/w
&fname._&nstock._trade_e/w
**/

%MACRO test_single_cfactor_higher_ret(factor_table, fname, bm_index, index_result_type,
				adjust_date, test_date,
				start_date, end_date, 
				annualized_factor,
				nstock=100);
	DATA &factor_table._c(keep = end_date stock_code &fname.);
		SET &factor_table.;
		IF not missing(&fname.);
	RUN;
	/** Step1: ȡ������ߵ�ǰN��(��һ��)*/
	%single_factor_score(raw_table=&factor_table._c, identity=stock_code, factor_name=&fname.,
		output_table=&factor_table._c, is_increase = 1, group_num =2);
	%cut_subset(input_table=&factor_table._c, colname=&fname., output_table=&factor_table._c,
					type=3, threshold=&nstock., is_top=1, is_cut=0);

	/** ��һЩ��Ϊ�������⣬���ƴ���0��������Ϊ0��֮���ڵ÷ּ�Ȩ������У���Ȩ��ʵ����Ϊ0 */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT *
		FROM &factor_table._c
		WHERE cut_mark = 1 AND &fname. > 0 AND abs(&fname.) >= 0.00001 AND 
		&fname. >= (SELECT min(&fname.) FROM &factor_table._c WHERE &fname._score = 2);
	QUIT;
	DATA &factor_table._c;
		SET tmp;
	RUN;

	/** Step2: ��Ȩ��� */
	PROC SQL;
		CREATE TABLE &factor_table._c2 AS
		SELECT end_date, stock_code, 1 AS weight
		FROM &factor_table._c
	QUIT;
	%construct_index(test_pool=&factor_table._c2, adjust_date=&adjust_date., test_date=&test_date.,
			bm_index_table=&bm_index.,   
			output_index_table=&fname._&nstock._index_e,
			output_stat_table=&fname._&nstock._stat_e,
			output_trade_table=&fname._&nstock._trade_e, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&start_date., 
			end_date=&end_date., 
			index_result_type = &index_result_type.,
			is_output=0, 
			annualized_factor=&annualized_factor.);


	/** Step3: �÷ּ�Ȩ��� */
	PROC SQL;
		CREATE TABLE &factor_table._c2 AS
		SELECT end_date, stock_code, &fname. AS weight
		FROM &factor_table._c;
	QUIT;

	%construct_index(test_pool=&factor_table._c2, adjust_date=&adjust_date., test_date=&test_date.,
			bm_index_table=&bm_index.,   
			output_index_table=&fname._&nstock._index_w,
			output_stat_table=&fname._&nstock._stat_w,
			output_trade_table=&fname._&nstock._trade_w, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&start_date., 
			end_date=&end_date., 
			index_result_type = &index_result_type.,
			is_output=0, 
			annualized_factor=&annualized_factor.);
	PROC SQL;
		DROP TABLE &factor_table._c, &factor_table._c2;
	QUIT;

%MEND test_single_cfactor_higher_ret;


/*** ģ��4����ɢ�÷֣�ȡ�÷�Ϊ�ض�ֵ�ı�����������Ȩ��� */
/** ���ﲻҪ��÷ּ�Ȩ����Ϊ�÷ֶ�����ͬ�ġ�ͬʱҲ����Ҫ��÷�>0���������*/
/** ���룺
(1) factor_table: ���ӵ÷��б�
(2) fname: ��������
(3) fname_value: �ض�����ֵ
(3) bm_index: ֻ�е�index_result_type=1ʱ����Ч��Ҫ��: end_date, daily_ret
(4) index_result_type: 1-������� 2-��������
(5) adjust_date: �������ڱ�(end_date)
(6) test_date: ������ڱ�(date)
(7) start_date: ��Ч������ʼ����
(8) end_date: ��Ч������������ (ע�⣺7��8һ�����test_date����ʼ�ͽ�������)
(9) annualized_factor: ����annualizedʱ��factor
**/
/** �����
&fname._&&fname_value._index_e
&fname._&fname_value._stat_e
&fname._&fname_value._trade_e
**/

%MACRO test_single_dfactor_higher_ret(factor_table, fname,fname_value, bm_index, index_result_type,
				adjust_date, test_date,
				start_date, end_date, 
				annualized_factor);
	DATA &factor_table._c(keep = end_date stock_code &fname.);
		SET &factor_table.;
		IF not missing(&fname.) AND &fname. = &fname_value.;
	RUN;

	/** Step1: ��Ȩ��� */
	PROC SQL;
		CREATE TABLE &factor_table._c2 AS
		SELECT end_date, stock_code, 1 AS weight
		FROM &factor_table._c
	QUIT;
	%construct_index(test_pool=&factor_table._c2, adjust_date=&adjust_date., test_date=&test_date.,
			bm_index_table=&bm_index.,   
			output_index_table=&fname._&fname_value._index_e,
			output_stat_table=&fname._&fname_value._stat_e,
			output_trade_table=&fname._&fname_value._trade_e, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&start_date., 
			end_date=&end_date., 
			index_result_type = &index_result_type.,
			is_output=0, 
			annualized_factor=&annualized_factor.);

	PROC SQL;
		DROP TABLE &factor_table._c, &factor_table._c2;
	QUIT;

%MEND test_single_dfactor_higher_ret;



/*** ģ��6�����ݵ÷ַ�ΪN�飬������ϣ�ѭ���������ӵĲ��ԡ�***/
/** ���룺
(1) factor_table: ���ӵ÷��б�
(2) fname: ��������
(3) bm_index: ֻ�е�index_result_type=1ʱ����Ч��Ҫ��: end_date, daily_ret
(4) index_result_type: 1-������� 2-��������
(5) adjust_date: �������ڱ�(end_date)
(6) test_date: ������ڱ�(date)
(7) start_date: ��Ч������ʼ����
(8) end_date: ��Ч������������ (ע�⣺7��8һ�����test_date����ʼ�ͽ�������)
(9) annualized_factor: ����annualizedʱ��factor
(10) ngroup: �ֳɵ�N��
(11) is_cut: 1- ��N��(�ʺ���������) 0- �ʺ���ɢ����(ֱ������fname��ֵ)

**/
/** �����(ÿ�����Ӷ��У�
&fname._g&rank._index
&fname._g&rank._stat
&fname._g&rank._trade
**/
%MACRO 	test_multiple_factor_group_ret(factor_table, bm_index, index_result_type,
				adjust_date, test_date,
				start_date, end_date, 
				annualized_factor,
				ngroup =3, is_cut=1,
				exclude_list=(''));
	PROC CONTENTS DATA = &factor_table. OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
            
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
		  %test_single_factor_group_ret(factor_table=&factor_table., fname=&fname., 
				bm_index=&bm_index., index_result_type=&index_result_type.,
				adjust_date=&adjust_date., test_date=&test_date.,
				start_date=&start_date., end_date=&end_date., 
				annualized_factor=&annualized_factor.,
				ngroup =&ngroup., is_cut=&is_cut.);
     %END;
	PROC SQL;
		DROP TABLE tt_varlist2;
	QUIT;
%MEND test_multiple_factor_group_ret;


/*** ģ��7�����ǰtopN���(�����������ض�����ֵ(��ɢ)������ϣ�ѭ���������ӵĲ��ԡ�***/
/** ע�⣺������Խ�test_single_cfactor_higher_ret��test_single_dfactor_higher_ret�ϲ�һ��
		ǰ���ʺ����������������ʺ���ɢ������������������ǰ���в���nstock��������factor_name�⣬������һ��
**/
/** ���룺
(1) factor_table: ���ӵ÷��б�
(2) fname: ��������
(3) bm_index: ֻ�е�index_result_type=1ʱ����Ч��Ҫ��: end_date, daily_ret
(4) index_result_type: 1-������� 2-��������
(5) adjust_date: �������ڱ�(end_date)
(6) test_date: ������ڱ�(date)
(7) start_date: ��Ч������ʼ����
(8) end_date: ��Ч������������ (ע�⣺7��8һ�����test_date����ʼ�ͽ�������)
(9) annualized_factor: ����annualizedʱ��factor
(10) var����is_continuous=1ʱ��var��Ӧnstock, ��is_continuous=0ʱvar��Ӧ&factor_name.
(11) is_continuous: 1-�������� 0-��ɢ����
(12) exclude_list
**/
/** �����
&fname._&nstock._index_e/w
&fname._&nstock._stat_e/w
&fname._&nstock._trade_e/w
**/

%MACRO 	test_multiple_factor_higher_ret(factor_table,bm_index, index_result_type,
				adjust_date, test_date,
				start_date, end_date, 
				annualized_factor,
				var,
				is_continuous=1,
				exclude_list=(''));
	PROC CONTENTS DATA = &factor_table. OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
	 %IF %SYSEVALF(&is_continuous.=1) %THEN %DO;        
    	 %DO i = 1 %TO &nfactors2.;
          	%LET fname =  %scan(&name_list2.,&i., ' ');
		  	%put testing &fname....;
		  	%test_single_cfactor_higher_ret(factor_table=&factor_table., fname=&fname., 
				bm_index=&bm_index., index_result_type=&index_result_type.,
				adjust_date=&adjust_date., test_date=&test_date.,
				start_date=&start_date., end_date=&end_date., 
				annualized_factor=&annualized_factor.,
				nstock=&var.);
     	%END;
	%END;
	%ELSE %IF %SYSEVALF(&is_continuous.=0) %THEN %DO;        
    	 %DO i = 1 %TO &nfactors2.;
          	%LET fname =  %scan(&name_list2.,&i., ' ');
		  	%put testing &fname....;
		  	%test_single_dfactor_higher_ret(factor_table=&factor_table., fname=&fname., 
				fname_value =&var.,
				bm_index=&bm_index., index_result_type=&index_result_type.,
				adjust_date=&adjust_date., test_date=&test_date.,
				start_date=&start_date., end_date=&end_date., 
				annualized_factor=&annualized_factor.);
     	%END;
	%END;

	PROC SQL;
		DROP TABLE tt_varlist2;
	QUIT;
%MEND test_multiple_factor_higher_ret;


/*** ģ��8�������������ӷ�3��Ľ��������Excel�鿴 */
/** �ú������ʺ���������������ɢ����û�취����ȡֵ�Ƿ�һ����1/2/3�����Ҳ�޷�ȷ��*/
/** ��Ҫ�����У�:test_multiple_factor_group_ret�����ִ�иú��� */
/** ���룺
(1) factor_table: ���ӱ������漰��������Ӧ����test_multiple_factor_ic���õ���factor_table���Ӽ���
(2) merge_var: ��Ҫ��test_multiple_factor_group_ret��������,��&fname._g&group._stat�еı�����
			�磺accum_ret/sd/ir/hit_ratio/index_draw/hit_ratio_m/turnover_uni/nstock������
(3) output_table: ��������������
	(i) factor: ��������
	(ii) g1_&merge_var.:��һ��ͳ�Ƶı���
	(iii) g2_&merge_var: �ڶ���
	(iv) g3_&merge_var.: ������
(4) exclude_list: factor_table�в����ǵ��ֶΣ�Ҫ���д
**/

%MACRO merge_factor_3group_result(factor_table, merge_var, output_table, exclude_list=(''));
	DATA &output_table.;
		ATTRIB
		factor LENGTH = $30.
		g1 LENGTH = 8
		g2 LENGTH = 8
		g3 LENGTH = 8
		;
		STOP;
	RUN;
	DATA rr_result;
		SET &factor_table.;
	RUN;

	PROC CONTENTS DATA = rr_result OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
          PROC SQL;
			CREATE TABLE tmp AS
			SELECT "&fname." AS factor, 
			A.&merge_var. AS g1,
			B.&merge_var. AS g2,
			C.&merge_var. AS g3
			FROM &fname._g1_stat A 
			LEFT JOIN &fname._g2_stat B
			ON A.year = B.year
			LEFT JOIN &fname._g3_stat C
			ON A.year = C.year
			WHERE A.year = 0;
		QUIT;
		DATA &output_table.;
			SET &output_table. tmp;
		RUN;
     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2;
	QUIT;
%MEND merge_factor_3group_result;

/*** ģ��9������Ȼ���topN��ϵĽ��������Excel�鿴 */
/** ��Ҫ�����У�:test_multiple_factor_higher_ret�����ִ�иú��� */
/** ���룺
(1) factor_table: ���ӱ������漰��������Ӧ����test_multiple_factor_ic���õ���factor_table���Ӽ���
(2) suffix: ����Ϊ &fname._&suffix. ��test_multiple_factor_high_ret�Ľ��
(2) merge_var: ��Ҫ��test_multiple_factor_group_ret��������,��&fname._g&group._stat�еı�����
			�磺accum_ret/sd/ir/hit_ratio/index_draw/hit_ratio_m/turnover_uni/nstock������
(3) output_table: ��������������
	(i) factor: ��������
	(ii) &merge_var.:��һ��ͳ�Ƶı���
(4) exclude_list: factor_table�в����ǵ��ֶΣ�Ҫ���д

**/

/*** ������������ȡǰN���Ľ��������Excel�鿴 */
%MACRO merge_result_higher_total(factor_table, suffix, merge_var, output_table, exclude_list);
	DATA rr_result;
		SET &factor_table.;
	RUN;

	PROC CONTENTS DATA = rr_result OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
      
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
		  %put &merge_var.;
		  DATA tmp;
		  	SET &fname._&suffix.(keep = year &merge_var.);
			LENGTH factor $ 30.;
			factor = "&fname.";
		  RUN;
		 PROC TRANSPOSE DATA = tmp OUT = tmp(drop = _NAME_) prefix = Y;
		 	BY factor;
			ID year;
			VAR &merge_var.;
		RUN;
		%IF %SYSEVALF(&i.=1) %THEN %DO;
			DATA &output_table.;
				SET tmp;
			RUN;
		%END;
		%ELSE %DO;
			DATA &output_table.;
				SET &output_table. tmp;
			RUN;
		%END;
     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2, tmp;
	QUIT;

%MEND merge_result_higher_total;

