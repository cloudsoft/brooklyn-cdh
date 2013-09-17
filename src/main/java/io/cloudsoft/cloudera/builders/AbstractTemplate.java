package io.cloudsoft.cloudera.builders;


@SuppressWarnings("unchecked")
public abstract class AbstractTemplate<T extends AbstractTemplate<?>> {

    public AbstractTemplate() {
    }

    protected String name;
    public T named(String name) {
        this.name = name;
        return (T)this;
    }
    public String getName() {
        return name;
    }

    public abstract Object build(ClouderaRestCaller caller);

}
