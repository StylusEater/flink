describe("Switch is an object", function() {
  var sw = Switch().create('');
  
  it("and must be passed a value", function() {
    expect(sw).toBe(null);
  });

  it("and can be initialized with an array value", function() {
    testValues = new Array('test');
    sw = Switch().create(testValues);

    expect(sw).not.toBe(null);
  });
  
  it("and only an array.", function() {
    sw = Switch().create('fail');
    
    expect(sw).toBe(null);
  });
  
  it("Is an iterator and has a next value.", function() {
    testValues = new Array('test','one');
    sw = Switch().create(testValues);
    
    expect(sw.next()).toEqual('test');
    expect(sw.next()).toEqual('one');
  });
  
  it("Is an iterator which when no values are left is true.", function() {
    testValue = new Array('foo');
    sw = Switch().create(testValue);
    
    sw.next();
    expect(sw.next()).toBe(true);
    
  });
  
  it("Is an iterator that can match values.", function() {
    testValues = new Array('test','one');
    sw = Switch().create(testValues);
    
    expect(sw.match('one')).toBe(true);
  });
   
});