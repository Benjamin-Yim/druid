/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
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
import org.junit.Assert;

public class StarrocksSelectTest5 extends TestCase {
    public void test_distribute_by() throws Exception {
        String sql = "select * from t where ds='20160303' and hour in ('18') ";//
        Assert.assertEquals("SELECT *"
                + "\nFROM t"
                + "\nWHERE ds = '20160303'"
                + "\n\tAND hour IN ('18')", SQLUtils.formatStarrocks(sql));
    }

}
