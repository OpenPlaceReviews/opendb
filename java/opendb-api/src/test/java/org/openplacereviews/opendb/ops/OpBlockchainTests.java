package org.openplacereviews.opendb.ops;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class OpBlockchainTests {

	public static final String SIMPLE_JSON = "{'a':1, 'b': 'b', 'c' : ['1', '2'], 'e' : {'a': {'a':3}} }";
	
	@BeforeClass
    public static void beforeAllTestMethods() {
         System.out.println("Invoked once before all test methods");
    }
 
    @Before
    public void beforeEachTestMethod() {
        System.out.println("Invoked before each test method");
    }
 
	
	@Test
	public void testSimpleFunctionEval() {
		assertEquals("3", "3");
	}
	
	@Test
	public void testSimpleFunctionEval2() {
		assertEquals("3", "3");
	}
}
