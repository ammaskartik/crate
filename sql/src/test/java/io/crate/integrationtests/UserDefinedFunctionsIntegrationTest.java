/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.integrationtests;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.Schemas;
import io.crate.metadata.settings.CrateSettings;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, randomDynamicTemplates = false)
public class UserDefinedFunctionsIntegrationTest extends SQLTransportIntegrationTest {


    private Object[][] rows = new Object[][]{
        new Object[]{1L, "Foo"},
        new Object[]{3L, "bar"}
    };
    private final String schema = "test";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(CrateSettings.UDF_ENABLED.settingName(), true).build();
    }

    @Before
    public void beforeTest() {
        // clustering by id into two shards must assure that the two inserted
        // records reside on two different nodes configured in the test setup.
        // So then it would be possible to test that a function is created and
        // applied on all of nodes.
        execute("create table test (id long, str string) clustered by(id) into 2 shards");
        execute("insert into test (id, str) values (?, ?)", rows);
        refresh();
    }

    @Test
    public void testCreateOverloadedFunction() throws Exception {
        try {
            execute("create function test.foo(long)" +
                " returns string language javascript as 'function foo(x) { return \"1\"; }'");
            waitForFunctionCreatedOnAll(schema, "foo", ImmutableList.of(DataTypes.LONG));

            execute("create function test.foo(string)" +
                " returns string language javascript as 'function foo(x) { return x; }'");
            waitForFunctionCreatedOnAll(schema, "foo", ImmutableList.of(DataTypes.STRING));

            execute("select test.foo(str), id from test order by id asc");
            assertThat(response.rowCount(), is(2L));
            assertThat(response.rows()[0][0], is("Foo"));
            assertThat(response.rows()[1][0], is("bar"));
        } finally {
            dropFunction(schema, "foo", ImmutableList.of(DataTypes.LONG));
            dropFunction(schema, "foo", ImmutableList.of(DataTypes.STRING));
        }
    }

    @Test
    public void testDropFunction() throws Exception {
        execute("create function custom(string)" +
            " returns string language javascript as 'function custom(x) { return x; }'");
        waitForFunctionCreatedOnAll(Schemas.DEFAULT_SCHEMA_NAME, "custom", ImmutableList.of(DataTypes.STRING));
        dropFunction(Schemas.DEFAULT_SCHEMA_NAME, "custom", ImmutableList.of(DataTypes.STRING));
    }

    private void dropFunction(@Nullable String schema, String name, List<DataType> types) throws Exception {
        execute(String.format(Locale.ENGLISH, "drop function %s.%s(%s)",
            schema, name, types.stream().map(DataType::getName).collect(Collectors.joining(", "))));
        assertThat(response.rowCount(), is(1L));
        waitForFunctionDeleted(schema, name, types);
    }
}
