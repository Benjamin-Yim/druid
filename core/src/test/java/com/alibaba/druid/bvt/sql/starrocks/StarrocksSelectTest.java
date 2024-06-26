/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.druid.bvt.sql.starrocks;

import com.alibaba.druid.sql.SQLUtils;
import junit.framework.TestCase;

public class StarrocksSelectTest extends TestCase {
    public void test_distribute_by() throws Exception {
        String sql = "select region from sale_detail distribute by region;";//
        assertEquals("SELECT region" //
                + "\nFROM sale_detail" //
                + "\nDISTRIBUTE BY region;", SQLUtils.formatStarrocks(sql));
    }

    public void test_distribute_by_1() throws Exception {
        String sql = " select region from sale_detail distribute by region sort by f1;";//
        assertEquals("SELECT region" //
                + "\nFROM sale_detail" //
                + "\nDISTRIBUTE BY region\n" +
                "SORT BY f1;", SQLUtils.formatStarrocks(sql));
    }

    public void test_distribute_by_2() throws Exception {
        String sql = " select region from sale_detail distribute by region sort by f1 asc;";//
        assertEquals("SELECT region" //
                + "\nFROM sale_detail" //
                + "\nDISTRIBUTE BY region\n" +
                "SORT BY f1 ASC;", SQLUtils.formatStarrocks(sql));
        assertEquals("select region" //
                + "\nfrom sale_detail" //
                + "\ndistribute by region\n" +
                "sort by f1 asc;", SQLUtils.formatStarrocks(sql, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

}
