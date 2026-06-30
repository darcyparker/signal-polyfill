import {describe, expect, it} from 'vitest';
import {Signal} from '../../src';

const microtask = () => Promise.resolve();

/**
 * Cooling (polyfill-layer GC restoration). alien's push core retains a forward
 * edge from a source to every computed that read it; a never-watched computed
 * that is dropped therefore leaks. Cooling parks standalone-read computeds and
 * detaches their dep edges on the next microtask so they become GC-able —
 * restoring the proposal's guarantee. It is asynchronous (microtask), so the
 * edge exists synchronously and is gone after a microtask.
 */
describe('unwatched-computed cooling', () => {
  it('synchronously the edge still exists (push core), gone after a microtask', async () => {
    const s = new Signal.State(0);
    {
      const c = new Signal.Computed(() => s.get() + 1);
      c.get();
      expect(Signal.subtle.introspectSinks(s)).toHaveLength(1); // not cooled yet
    }
    await microtask();
    expect(Signal.subtle.introspectSinks(s)).toHaveLength(0); // cooled → GC-able
    expect(Signal.subtle.hasSinks(s)).toBe(false);
  });

  it('cools many throwaway computeds', async () => {
    const s = new Signal.State(0);
    for (let i = 0; i < 1000; i++) {
      const c = new Signal.Computed(() => s.get() + 1);
      c.get();
    }
    await microtask();
    expect(Signal.subtle.introspectSinks(s)).toHaveLength(0);
  });

  it('does NOT cool a watched computed', async () => {
    const s = new Signal.State(0);
    const c = new Signal.Computed(() => s.get() + 1);
    const w = new Signal.subtle.Watcher(() => {});
    w.watch(c);
    c.get();
    await microtask();
    expect(Signal.subtle.introspectSinks(s).length).toBeGreaterThan(0); // live → retained
    w.unwatch(c);
  });

  it('warms up: a cooled computed recomputes with the latest value on next read', async () => {
    const s = new Signal.State(1);
    const c = new Signal.Computed(() => s.get() * 10);
    expect(c.get()).toBe(10);
    await microtask(); // cooled (detached from s)
    s.set(2);
    expect(c.get()).toBe(20); // warms up: recomputes, re-links
    await microtask();
    s.set(3);
    expect(c.get()).toBe(30);
  });

  it('cools a transitive unwatched chain back to the source', async () => {
    const s = new Signal.State(0);
    {
      const a = new Signal.Computed(() => s.get() + 1);
      const b = new Signal.Computed(() => a.get() + 1);
      b.get(); // only b is read at top level; a is read inside b
    }
    await microtask();
    expect(Signal.subtle.introspectSinks(s)).toHaveLength(0); // chain unwound via unwatched cascade
  });
});
