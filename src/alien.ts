import {
  ReactiveFlags,
  createReactiveSystem,
  type ReactiveNode,
  type Link,
} from 'alien-signals/system';

export namespace Signal {
  export let isState: (s: any) => boolean,
    isComputed: (s: any) => boolean,
    isWatcher: (s: any) => boolean;

  const WATCHER_PLACEHOLDER = Symbol('watcher') as any;

  const enum EffectFlags {
    Queued = 1 << 6,
  }

  const {
    link,
    unlink,
    propagate,
    checkDirty,
    endTracking,
    startTracking,
    shallowPropagate,
  } = createReactiveSystem({
    update(node: State | Computed) {
      return node.update();
    },
    notify(node: subtle.Watcher) {
      const flags = node.flags;
      if (!(flags & EffectFlags.Queued)) {
        node.flags = flags | EffectFlags.Queued;
        queuedEffects[queuedEffectsLength++] = node;
      }
    },
    unwatched(node) {
      let toRemove = node.deps;
      if (toRemove !== undefined) {
        do {
          toRemove = unlink(toRemove, node);
        } while (toRemove !== undefined);
        node.flags |= ReactiveFlags.Dirty;
      }
    },
  });
  const queuedEffects: subtle.Watcher[] = [];

  let notifyIndex = 0;
  let queuedEffectsLength = 0;
  let activeSub: ReactiveNode | undefined;

  function flush(): void {
    while (notifyIndex < queuedEffectsLength) {
      const effect = queuedEffects[notifyIndex];
      // @ts-expect-error
      queuedEffects[notifyIndex++] = undefined;
      effect.flags &= ~EffectFlags.Queued;
      effect.run();
    }
    notifyIndex = 0;
    queuedEffectsLength = 0;
  }

  export function untrack<T>(fn: () => T) {
    const prevSub = activeSub;
    activeSub = undefined;
    try {
      return fn();
    } finally {
      activeSub = prevSub;
    }
  }

  export class State<T = any> implements ReactiveNode {
    subs: Link | undefined = undefined;
    subsTail: Link | undefined = undefined;
    flags: ReactiveFlags = ReactiveFlags.Mutable;
    watchCount = 0;
    previousValue: T;

    #brand() { }
    static {
      isState = (s) => typeof s === 'object' && #brand in s;
    }

    constructor(
      private value: T,
      private options?: Options<T>,
    ) {
      this.previousValue = value;
      if (options?.equals !== undefined) {
        this.equals = options.equals;
      }
    }

    equals(t: T, t2: T): boolean {
      return Object.is(t, t2);
    }

    onWatched() {
      if (this.watchCount++ === 0) {
        this.options?.[subtle.watched]?.call(this);
      }
    }

    onUnwatched() {
      if (--this.watchCount === 0) {
        this.options?.[subtle.unwatched]?.call(this);
      }
    }

    update() {
      this.flags &= ~ReactiveFlags.Dirty;
      return !this.equals(this.previousValue, this.previousValue = this.value);
    }

    get() {
      if (!isState(this)) {
        throw new TypeError('Wrong receiver type for Signal.State.prototype.get');
      }
      if (activeSub === WATCHER_PLACEHOLDER) {
        throw new Error('Cannot read from state inside watcher');
      }
      if (this.flags & ReactiveFlags.Dirty) {
        if (this.update()) {
          const subs = this.subs;
          if (subs !== undefined) {
            shallowPropagate(subs);
          }
        }
      }
      if (activeSub !== undefined) {
        const lastLink = this.subsTail;
        link(this, activeSub);
        const newLink = this.subsTail!;
        if (newLink !== lastLink) {
          const newSub = newLink.sub;
          if (newSub instanceof Computed && newSub.watchCount) {
            this.onWatched();
          }
        }
      }
      return this.value;
    }

    set(value: T): void {
      if (!isState(this)) {
        throw new TypeError('Wrong receiver type for Signal.State.prototype.set');
      }
      if (activeSub === WATCHER_PLACEHOLDER) {
        throw new Error('Cannot write to state inside watcher');
      }
      if (!this.equals(this.value, value)) {
        this.value = value;
        this.flags = ReactiveFlags.Mutable | ReactiveFlags.Dirty;
        const subs = this.subs;
        if (subs !== undefined) {
          propagate(subs);
          flush();
        }
      }
    }
  }

  export class Computed<T = any> implements ReactiveNode {
    subs: Link | undefined = undefined;
    subsTail: Link | undefined = undefined;
    deps: Link | undefined = undefined;
    depsTail: Link | undefined = undefined;
    flags = ReactiveFlags.Mutable | ReactiveFlags.Dirty;
    isError = true;
    watchCount = 0;
    value: T | undefined = undefined;

    #brand() { }
    static {
      isComputed = (c: any) => typeof c === 'object' && #brand in c;
    }

    constructor(
      private getter: () => T,
      private options?: Options<T>,
    ) {
      if (options?.equals !== undefined) {
        this.equals = options.equals;
      }
    }

    equals(t: T, t2: T): boolean {
      return Object.is(t, t2);
    }

    onWatched() {
      if (this.watchCount++ === 0) {
        this.options?.[subtle.watched]?.call(this);
        for (let link = this.deps; link !== undefined; link = link.nextDep) {
          const dep = link.dep as AnySignal;
          dep.onWatched();
        }
      }
    }

    onUnwatched() {
      if (--this.watchCount === 0) {
        this.options?.[subtle.unwatched]?.call(this);
        for (let link = this.deps; link !== undefined; link = link.nextDep) {
          const dep = link.dep as AnySignal;
          dep.onUnwatched();
        }
      }
    }

    get() {
      if (!isComputed(this)) {
        throw new TypeError('Wrong receiver type for Signal.Computed.prototype.get');
      }
      if (activeSub === WATCHER_PLACEHOLDER) {
        throw new Error('Cannot read from computed inside watcher');
      }
      let flags = this.flags;
      if (flags & ReactiveFlags.RecursedCheck) {
        throw new Error('Cycles detected');
      }
      if (
        flags & ReactiveFlags.Dirty
        || (flags & ReactiveFlags.Pending && checkDirty(this.deps!, this))
      ) {
        if (this.update()) {
          const subs = this.subs;
          if (subs !== undefined) {
            shallowPropagate(subs);
          }
        }
      } else if (flags & ReactiveFlags.Pending) {
        this.flags = flags & ~ReactiveFlags.Pending;
      }
      if (activeSub !== undefined) {
        const lastLink = this.subsTail;
        link(this, activeSub);
        const newLink = this.subsTail!;
        if (newLink !== lastLink) {
          const newSub = newLink.sub;
          if (newSub instanceof Computed && newSub.watchCount) {
            this.onWatched();
          }
        }
      }
      if (this.isError) {
        throw this.value;
      }
      return this.value!;
    }

    update(): boolean {
      const prevSub = activeSub;
      activeSub = this;
      startTracking(this);
      const oldValue = this.value;
      try {
        const newValue = this.getter();
        if (this.isError || !this.equals(oldValue!, newValue)) {
          this.isError = false;
          this.value = newValue;
          return true;
        }
        return false;
      } catch (err) {
        if (!this.isError || !this.equals(oldValue!, err as any)) {
          this.isError = true;
          this.value = err as any;
          return true;
        }
        return false;
      } finally {
        if (this.watchCount) {
          for (
            let link = this.depsTail !== undefined ? this.depsTail.nextDep : this.deps;
            link !== undefined;
            link = link.nextDep
          ) {
            const dep = link.dep as AnySignal;
            dep.onUnwatched();
          }
        }
        activeSub = prevSub;
        endTracking(this);
      }
    }
  }

  type AnySignal<T = any> = State<T> | Computed<T>;
  type AnySink = Computed<any> | subtle.Watcher;

  export namespace subtle {
    export class Watcher implements ReactiveNode {
      deps: Link | undefined = undefined;
      depsTail: Link | undefined = undefined;
      flags = ReactiveFlags.Watching;
      watchList = new Map<AnySignal, Link>();

      #brand() { }
      static {
        isWatcher = (w: any): w is Watcher => #brand in w;
      }

      constructor(private fn: () => void) { }

      run() {
        const prevSub = activeSub;
        activeSub = WATCHER_PLACEHOLDER;
        try {
          this.fn();
        } finally {
          activeSub = prevSub;
        }
      }

      #assertSignals(signals: AnySignal[]): void {
        for (const signal of signals) {
          if (!isComputed(signal) && !isState(signal)) {
            throw new TypeError('Called watch/unwatch without a Computed or State argument');
          }
        }
      }

      watch(...signals: AnySignal[]): void {
        if (!isWatcher(this)) {
          throw new TypeError('Called watch without Watcher receiver');
        }
        this.#assertSignals(signals);

        for (const signal of signals) {
          if (this.watchList.has(signal)) {
            continue;
          }
          signal.onWatched();
          link(signal, this);
          this.watchList.set(signal, this.depsTail!);
        }
      }

      unwatch(...signals: AnySignal[]): void {
        if (!isWatcher(this)) {
          throw new TypeError('Called unwatch without Watcher receiver');
        }
        this.#assertSignals(signals);

        for (const signal of signals) {
          const link = this.watchList.get(signal);
          if (link === undefined) {
            continue;
          }
          signal.onUnwatched();
          unlink(link, this);
          this.watchList.delete(signal);
        }
      }

      getPending() {
        if (!isWatcher(this)) {
          throw new TypeError('Called getPending without Watcher receiver');
        }
        const arr: AnySignal[] = [];
        for (let link = this.deps; link !== undefined; link = link.nextDep) {
          const source = link.dep;
          if (
            source.flags & (ReactiveFlags.Dirty | ReactiveFlags.Pending) &&
            source instanceof Computed
          ) {
            arr.push(link.dep as AnySignal);
          }
        }
        return arr;
      }
    }

    export function hasSinks(signal: AnySignal) {
      if (!isComputed(signal) && !isState(signal)) {
        throw new TypeError('Called hasSinks without a Signal argument');
      }
      return signal.watchCount > 0;
    }

    export function hasSources(signal: AnySink) {
      if (!isComputed(signal) && !isWatcher(signal)) {
        throw new TypeError('Called hasSources without a Computed or Watcher argument');
      }
      return signal.depsTail !== undefined;
    }

    export function introspectSinks(signal: AnySignal): AnySink[] {
      if (!isComputed(signal) && !isState(signal)) {
        throw new TypeError('Called introspectSinks without a Signal argument');
      }
      const arr: AnySink[] = [];
      for (let link = signal.subs; link !== undefined; link = link.nextSub) {
        arr.push(link.sub as AnySink);
      }
      return arr;
    }

    export function introspectSources(sink: AnySink): AnySignal[] {
      if (!isComputed(sink) && !isWatcher(sink)) {
        throw new TypeError('Called introspectSources without a Computed or Watcher argument');
      }
      const arr: AnySignal[] = [];
      for (let link = sink.deps; link !== undefined; link = link.nextDep) {
        arr.push(link.dep as AnySignal);
      }
      return arr;
    }

    // Hooks to observe being watched or no longer watched
    export const watched = Symbol('watched');
    export const unwatched = Symbol('unwatched');
  }

  export interface Options<T> {
    // Custom comparison function between old and new value. Default: Object.is.
    // The signal is passed in as an optionally-used third parameter for context.
    equals?: (this: AnySignal, t: T, t2: T) => boolean;

    // Callback called when hasSinks becomes true, if it was previously false
    [subtle.watched]?: (this: AnySignal) => void;

    // Callback called whenever hasSinks becomes false, if it was previously true
    [subtle.unwatched]?: (this: AnySignal) => void;
  }
}
