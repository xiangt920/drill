/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Generated by http://code.google.com/p/protostuff/ ... DO NOT EDIT!
// Generated from protobuf

package org.apache.drill.exec.proto.beans;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import com.dyuproject.protostuff.GraphIOUtil;
import com.dyuproject.protostuff.Input;
import com.dyuproject.protostuff.Message;
import com.dyuproject.protostuff.Output;
import com.dyuproject.protostuff.Schema;

public final class Jar implements Externalizable, Message<Jar>, Schema<Jar>
{

    public static Schema<Jar> getSchema()
    {
        return DEFAULT_INSTANCE;
    }

    public static Jar getDefaultInstance()
    {
        return DEFAULT_INSTANCE;
    }

    static final Jar DEFAULT_INSTANCE = new Jar();

    
    private String name;
    private List<String> functionSignature;

    public Jar()
    {
        
    }

    // getters and setters

    // name

    public String getName()
    {
        return name;
    }

    public Jar setName(String name)
    {
        this.name = name;
        return this;
    }

    // functionSignature

    public List<String> getFunctionSignatureList()
    {
        return functionSignature;
    }

    public Jar setFunctionSignatureList(List<String> functionSignature)
    {
        this.functionSignature = functionSignature;
        return this;
    }

    // java serialization

    public void readExternal(ObjectInput in) throws IOException
    {
        GraphIOUtil.mergeDelimitedFrom(in, this, this);
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        GraphIOUtil.writeDelimitedTo(out, this, this);
    }

    // message method

    public Schema<Jar> cachedSchema()
    {
        return DEFAULT_INSTANCE;
    }

    // schema methods

    public Jar newMessage()
    {
        return new Jar();
    }

    public Class<Jar> typeClass()
    {
        return Jar.class;
    }

    public String messageName()
    {
        return Jar.class.getSimpleName();
    }

    public String messageFullName()
    {
        return Jar.class.getName();
    }

    public boolean isInitialized(Jar message)
    {
        return true;
    }

    public void mergeFrom(Input input, Jar message) throws IOException
    {
        for(int number = input.readFieldNumber(this);; number = input.readFieldNumber(this))
        {
            switch(number)
            {
                case 0:
                    return;
                case 1:
                    message.name = input.readString();
                    break;
                case 2:
                    if(message.functionSignature == null)
                        message.functionSignature = new ArrayList<String>();
                    message.functionSignature.add(input.readString());
                    break;
                default:
                    input.handleUnknownField(number, this);
            }   
        }
    }


    public void writeTo(Output output, Jar message) throws IOException
    {
        if(message.name != null)
            output.writeString(1, message.name, false);

        if(message.functionSignature != null)
        {
            for(String functionSignature : message.functionSignature)
            {
                if(functionSignature != null)
                    output.writeString(2, functionSignature, true);
            }
        }
    }

    public String getFieldName(int number)
    {
        switch(number)
        {
            case 1: return "name";
            case 2: return "functionSignature";
            default: return null;
        }
    }

    public int getFieldNumber(String name)
    {
        final Integer number = __fieldMap.get(name);
        return number == null ? 0 : number.intValue();
    }

    private static final java.util.HashMap<String,Integer> __fieldMap = new java.util.HashMap<String,Integer>();
    static
    {
        __fieldMap.put("name", 1);
        __fieldMap.put("functionSignature", 2);
    }
    
}
