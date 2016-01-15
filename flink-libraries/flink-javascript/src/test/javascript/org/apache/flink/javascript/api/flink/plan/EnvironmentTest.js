describe("Environment ", function() {
  var env = Environment();
  
  it("is an object ", function() {
    expect(env).not.toBe(null);
  });
  
  it("and it has various properties.", function() {
    expect(env._counter).toEqual(0);
    expect(env._dop).toEqual(-1);
    expect(env._local_mode).toBe(false);
    expect(env._debug_mode).toBe(false);
    expect(env._retry).toEqual(0);
    expect(env._sources).toEqual(new Array());
    expect(env._sets).toEqual(new Array());
    expect(env._sinks).toEqual(new Array());
    expect(env._broadcast).toEqual(new Array());
    expect(env._types).toEqual(new Array());
  });
  
  
});