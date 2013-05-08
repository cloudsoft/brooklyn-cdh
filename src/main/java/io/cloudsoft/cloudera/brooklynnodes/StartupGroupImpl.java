package io.cloudsoft.cloudera.brooklynnodes;

import static brooklyn.util.GroovyJavaMethods.truth;
import groovy.transform.InheritConstructors;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.AbstractEntity;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityLocal;
import brooklyn.entity.trait.Startable;
import brooklyn.entity.trait.StartableMethods;
import brooklyn.location.Location;
import brooklyn.management.Task;
import brooklyn.util.MutableMap;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

@InheritConstructors
public class StartupGroupImpl extends AbstractEntity implements StartupGroup {

    public static final Logger log = LoggerFactory.getLogger(StartupGroupImpl.class);

    @Override public void start(Collection<? extends Location> locations) { 
        startWithEnough(this, locations, 0, 0.75); 
    }
    
    @Override public void stop() { StartableMethods.stop(this); }
    @Override public void restart() {
        stop();
        start(ImmutableList.<Location>of());
    }

    /** variant of start which allows some failures */
    public static void startWithEnough(Entity e, Collection<? extends Location> locations, int minCount, double minPercent) {
        log.info("Starting entity "+e+" at "+locations);
        Iterable<Entity> startables = Iterables.filter(e.getChildren(), Predicates.instanceOf(Startable.class));

        Throwable error = null;
        if (!Iterables.isEmpty(startables) && truth(locations) && !locations.isEmpty()) {
            Task start = Entities.invokeEffectorList((EntityLocal)e, startables, Startable.START, MutableMap.of("locations", locations));
            try {
                start.get();
            } catch (ExecutionException ee) {
                error = ee.getCause();
            } catch (InterruptedException ee) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(ee);
            }
            if (error != null) {
                //some failed
            	int numUp = sizeOf(Iterables.filter(startables, new Predicate<Entity>() {
            		public boolean apply(Entity input) {
            			return Boolean.TRUE.equals(input.getAttribute(Startable.SERVICE_UP));
            		}
            	}));
                int numTotal = sizeOf(startables);
                if (numUp<minCount) {
                    log.warn("StartupGroup "+e+" only got "+numUp+" of "+numTotal+" children up ("+minCount+" needed); failing, with "+error);
                    throw Throwables.propagate(error);
                }
                if (minPercent*numTotal>numUp) {
                    log.warn("StartupGroup "+e+" only got "+numUp+" of "+numTotal+" ("+(numUp*100/numTotal)+"%, "+100*minCount+"% needed) children up; failing, with "+error);
                    throw Throwables.propagate(error);
                }
            }
        }
    }

    public static int sizeOf(Iterable<?> x) {
    	return Iterables.size(x);
    }
}
