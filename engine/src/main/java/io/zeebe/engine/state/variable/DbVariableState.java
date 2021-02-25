/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.variable;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.TransactionContext;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.DbCompositeKey;
import io.zeebe.db.impl.DbLong;
import io.zeebe.db.impl.DbString;
import io.zeebe.engine.state.KeyGenerator;
import io.zeebe.engine.state.ZbColumnFamilies;
import io.zeebe.engine.state.instance.ParentScopeKey;
import io.zeebe.engine.state.instance.TemporaryVariables;
import io.zeebe.engine.state.mutable.MutableVariableState;
import io.zeebe.msgpack.spec.MsgPackReader;
import io.zeebe.msgpack.spec.MsgPackWriter;
import io.zeebe.util.buffer.BufferUtil;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ObjectHashSet;
import org.agrona.concurrent.UnsafeBuffer;

public class DbVariableState implements MutableVariableState {

  private final MsgPackReader reader = new MsgPackReader();
  private final MsgPackWriter writer = new MsgPackWriter();
  private final ExpandableArrayBuffer documentResultBuffer = new ExpandableArrayBuffer();
  private final DirectBuffer resultView = new UnsafeBuffer(0, 0);

  // (child scope key) => (parent scope key)
  private final ColumnFamily<DbLong, ParentScopeKey> childParentColumnFamily;
  private final DbLong childKey;
  private final ParentScopeKey parentKey = new ParentScopeKey();

  // (scope key, variable name) => (variable value)
  private final ColumnFamily<DbCompositeKey<DbLong, DbString>, VariableInstance>
      variablesColumnFamily;
  private final DbCompositeKey<DbLong, DbString> scopeKeyVariableNameKey;
  private final DbLong scopeKey;
  private final DbString variableName;

  // (scope key) => (temporaryVariables)
  private final ColumnFamily<DbLong, TemporaryVariables> temporaryVariableStoreColumnFamily;
  private final TemporaryVariables temporaryVariables = new TemporaryVariables();

  private final VariableInstance newVariable = new VariableInstance();
  private final DirectBuffer variableNameView = new UnsafeBuffer(0, 0);

  // collecting variables
  private final ObjectHashSet<DirectBuffer> collectedVariables = new ObjectHashSet<>();
  private final ObjectHashSet<DirectBuffer> variablesToCollect = new ObjectHashSet<>();

  // setting variables
  private final IndexedDocument indexedDocument = new IndexedDocument(reader);
  private final KeyGenerator keyGenerator;

  private VariableListener listener;
  private int variableCount = 0;

  public DbVariableState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb,
      final TransactionContext transactionContext,
      final KeyGenerator keyGenerator) {
    this.keyGenerator = keyGenerator;

    childKey = new DbLong();
    childParentColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.ELEMENT_INSTANCE_CHILD_PARENT,
            transactionContext,
            childKey,
            parentKey);

    scopeKey = new DbLong();
    variableName = new DbString();
    scopeKeyVariableNameKey = new DbCompositeKey<>(scopeKey, variableName);
    variablesColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.VARIABLES,
            transactionContext,
            scopeKeyVariableNameKey,
            new VariableInstance());

    temporaryVariableStoreColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.TEMPORARY_VARIABLE_STORE,
            transactionContext,
            scopeKey,
            temporaryVariables);
  }

  @Override
  public void setVariablesLocalFromDocument(
      final long scopeKey, final long workflowKey, final DirectBuffer document) {
    indexedDocument.index(document);
    if (indexedDocument.isEmpty()) {
      return;
    }

    for (final DocumentEntry entry : indexedDocument) {
      setVariableLocal(scopeKey, workflowKey, entry.getName(), entry.getValue());
    }
  }

  @Override
  public void setVariableLocal(
      final long scopeKey,
      final long workflowKey,
      final DirectBuffer name,
      final DirectBuffer value) {
    setVariableLocal(scopeKey, workflowKey, name, 0, name.capacity(), value, 0, value.capacity());
  }

  @Override
  public void setVariableLocal(
      final long scopeKey,
      final long workflowKey,
      final DirectBuffer name,
      final DirectBuffer value,
      final int valueOffset,
      final int valueLength) {
    setVariableLocal(
        scopeKey, workflowKey, name, 0, name.capacity(), value, valueOffset, valueLength);
  }

  @Override
  public void setVariableLocal(
      final long scopeKey,
      final long workflowKey,
      final DirectBuffer name,
      final int nameOffset,
      final int nameLength,
      final DirectBuffer value,
      final int valueOffset,
      final int valueLength) {

    newVariable.reset();
    newVariable.setValue(value, valueOffset, valueLength);

    final VariableInstance currentVariable =
        getVariableLocal(scopeKey, name, nameOffset, nameLength);

    if (currentVariable == null) {
      newVariable.setKey(keyGenerator.nextKey());
      variablesColumnFamily.put(scopeKeyVariableNameKey, newVariable);

      if (listener != null) {
        final long rootScopeKey = getRootScopeKey(scopeKey);
        listener.onCreate(
            newVariable.getKey(),
            workflowKey,
            variableName.getBuffer(),
            newVariable.getValue(),
            scopeKey,
            rootScopeKey);
      }

    } else if (!BufferUtil.equals(currentVariable.getValue(), newVariable.getValue())) {
      newVariable.setKey(currentVariable.getKey());
      variablesColumnFamily.put(scopeKeyVariableNameKey, newVariable);

      if (listener != null) {
        final long rootScopeKey = getRootScopeKey(scopeKey);
        listener.onUpdate(
            newVariable.getKey(),
            workflowKey,
            variableName.getBuffer(),
            newVariable.getValue(),
            scopeKey,
            rootScopeKey);
      }

    } else {
      // not updated
    }
  }

  @Override
  public void setVariablesFromDocument(
      final long scopeKey, final long workflowKey, final DirectBuffer document) {
    indexedDocument.index(document);
    if (indexedDocument.isEmpty()) {
      return;
    }

    long currentScope = scopeKey;
    long parentScope;

    while ((parentScope = getParentScopeKey(currentScope)) > 0) {
      final Iterator<DocumentEntry> entryIterator = indexedDocument.iterator();

      while (entryIterator.hasNext()) {
        final DocumentEntry entry = entryIterator.next();
        final boolean hasVariable = hasVariableLocal(currentScope, entry.getName());

        if (hasVariable) {
          setVariableLocal(currentScope, workflowKey, entry.getName(), entry.getValue());
          entryIterator.remove();
        }
      }
      currentScope = parentScope;
    }

    for (final DocumentEntry entry : indexedDocument) {
      setVariableLocal(currentScope, workflowKey, entry.getName(), entry.getValue());
    }
  }

  @Override
  public void createScope(final long childKey, final long parentKey) {
    this.childKey.wrapLong(childKey);
    this.parentKey.set(parentKey);

    childParentColumnFamily.put(this.childKey, this.parentKey);
  }

  @Override
  public void removeScope(final long scopeKey) {
    this.scopeKey.wrapLong(scopeKey);

    removeAllVariables(scopeKey);

    childKey.wrapLong(scopeKey);
    childParentColumnFamily.delete(childKey);
  }

  @Override
  public void removeAllVariables(final long scopeKey) {
    visitVariablesLocal(
        scopeKey,
        dbString -> true,
        (dbString, variable1) -> variablesColumnFamily.delete(scopeKeyVariableNameKey),
        () -> false);
  }

  @Override
  public void setTemporaryVariables(final long scopeKey, final DirectBuffer variables) {
    this.scopeKey.wrapLong(scopeKey);
    temporaryVariables.set(variables);
    temporaryVariableStoreColumnFamily.put(this.scopeKey, temporaryVariables);
  }

  @Override
  public void removeTemporaryVariables(final long scopeKey) {
    this.scopeKey.wrapLong(scopeKey);
    temporaryVariableStoreColumnFamily.delete(this.scopeKey);
  }

  @Override
  public DirectBuffer getVariableLocal(final long scopeKey, final DirectBuffer name) {
    final VariableInstance variable = getVariableLocal(scopeKey, name, 0, name.capacity());

    if (variable != null) {
      return variable.getValue();
    } else {
      return null;
    }
  }

  /**
   * Find the variable with the given name. If the variable is not present in the given scope then
   * it looks in the parent scope and continues until it is found.
   *
   * @param scopeKey the key of the variable scope to start from
   * @param name the name of the variable
   * @return the value of the variable, or {@code null} if it is not present in the variable scope
   */
  @Override
  public DirectBuffer getVariable(final long scopeKey, final DirectBuffer name) {
    return getVariable(scopeKey, name, 0, name.capacity());
  }

  /**
   * Find the variable with the given name. If the variable is not present in the given scope then
   * it looks in the parent scope and continues until it is found.
   *
   * @param scopeKey the key of the variable scope to start from
   * @param name the buffer that contains the name of the variable
   * @param nameOffset the offset of name in the buffer
   * @param nameLength the length of the name in the buffer
   * @return the value of the variable, or {@code null} if it is not present in the variable scope
   */
  @Override
  public DirectBuffer getVariable(
      final long scopeKey, final DirectBuffer name, final int nameOffset, final int nameLength) {

    long currentScopeKey = scopeKey;
    do {
      final VariableInstance variable =
          getVariableLocal(currentScopeKey, name, nameOffset, nameLength);

      if (variable != null) {
        return variable.getValue();
      }

      currentScopeKey = getParentScopeKey(currentScopeKey);
    } while (currentScopeKey >= 0);

    return null;
  }

  @Override
  public DirectBuffer getVariablesAsDocument(final long scopeKey) {

    collectedVariables.clear();
    writer.wrap(documentResultBuffer, 0);

    writer.reserveMapHeader();

    visitVariables(
        scopeKey,
        name -> !collectedVariables.contains(name.getBuffer()),
        (name, value) -> {
          final DirectBuffer variableNameBuffer = name.getBuffer();
          writer.writeString(variableNameBuffer);
          writer.writeRaw(value.getValue());

          // must create a new name wrapper, because we keep them all in the hashset at the same
          // time
          final MutableDirectBuffer nameView = new UnsafeBuffer(variableNameBuffer);
          collectedVariables.add(nameView);
        },
        () -> false);

    writer.writeReservedMapHeader(0, collectedVariables.size());

    resultView.wrap(documentResultBuffer, 0, writer.getOffset());
    return resultView;
  }

  @Override
  public DirectBuffer getVariablesAsDocument(
      final long scopeKey, final Collection<DirectBuffer> names) {

    variablesToCollect.clear();
    variablesToCollect.addAll(names);

    writer.wrap(documentResultBuffer, 0);

    writer.reserveMapHeader();

    visitVariables(
        scopeKey,
        name -> variablesToCollect.contains(name.getBuffer()),
        (name, value) -> {
          writer.writeString(name.getBuffer());
          writer.writeRaw(value.getValue());

          variablesToCollect.remove(name.getBuffer());
        },
        variablesToCollect::isEmpty);

    writer.writeReservedMapHeader(0, names.size() - variablesToCollect.size());

    resultView.wrap(documentResultBuffer, 0, writer.getOffset());
    return resultView;
  }

  @Override
  public DirectBuffer getVariablesLocalAsDocument(final long scopeKey) {

    writer.wrap(documentResultBuffer, 0);

    writer.reserveMapHeader();

    variableCount = 0;

    visitVariablesLocal(
        scopeKey,
        name -> true,
        (name, value) -> {
          writer.writeString(name.getBuffer());
          writer.writeRaw(value.getValue());

          variableCount += 1;
        },
        () -> false);

    writer.writeReservedMapHeader(0, variableCount);

    resultView.wrap(documentResultBuffer, 0, writer.getOffset());
    return resultView;
  }

  @Override
  public DirectBuffer getTemporaryVariables(final long scopeKey) {
    this.scopeKey.wrapLong(scopeKey);
    final TemporaryVariables variables = temporaryVariableStoreColumnFamily.get(this.scopeKey);

    return variables == null || variables.get().byteArray() == null ? null : variables.get();
  }

  @Override
  public boolean isEmpty() {
    return variablesColumnFamily.isEmpty()
        && childParentColumnFamily.isEmpty()
        && temporaryVariableStoreColumnFamily.isEmpty();
  }

  @Override
  public boolean hasVariableLocal(final long scopeKey, final DirectBuffer name) {
    this.scopeKey.wrapLong(scopeKey);
    variableNameView.wrap(name, 0, name.capacity());
    variableName.wrapBuffer(variableNameView);

    return variablesColumnFamily.exists(scopeKeyVariableNameKey);
  }

  @Override
  public void setListener(final VariableListener listener) {
    if (this.listener != null) {
      throw new IllegalStateException("variable listener is already set");
    }

    this.listener = listener;
  }

  @Override
  public long getParentScopeKey(final long childScopeKey) {
    childKey.wrapLong(childScopeKey);

    final ParentScopeKey parentScopeKey = childParentColumnFamily.get(childKey);
    return parentScopeKey != null ? parentScopeKey.get() : NO_PARENT;
  }

  private VariableInstance getVariableLocal(
      final long scopeKey, final DirectBuffer name, final int nameOffset, final int nameLength) {
    this.scopeKey.wrapLong(scopeKey);
    variableNameView.wrap(name, nameOffset, nameLength);
    variableName.wrapBuffer(variableNameView);

    return variablesColumnFamily.get(scopeKeyVariableNameKey);
  }

  /**
   * Like {@link #visitVariablesLocal(long, Predicate, BiConsumer, BooleanSupplier)} but walks up
   * the scope hierarchy.
   */
  private void visitVariables(
      final long scopeKey,
      final Predicate<DbString> filter,
      final BiConsumer<DbString, VariableInstance> variableConsumer,
      final BooleanSupplier completionCondition) {
    long currentScope = scopeKey;

    boolean completed;
    do {
      completed = visitVariablesLocal(currentScope, filter, variableConsumer, completionCondition);

      currentScope = getParentScopeKey(currentScope);

    } while (!completed && currentScope >= 0);
  }

  /**
   * Provides all variables of a scope to the given consumer until a condition is met.
   *
   * @param variableFilter evaluated with the name of each variable; the variable is consumed only
   *     if the filter returns true
   * @param variableConsumer a consumer that receives variable name and value
   * @param completionCondition evaluated after every consumption; if true, consumption stops.
   * @return true if the completion condition was met
   */
  private boolean visitVariablesLocal(
      final long scopeKey,
      final Predicate<DbString> variableFilter,
      final BiConsumer<DbString, VariableInstance> variableConsumer,
      final BooleanSupplier completionCondition) {
    this.scopeKey.wrapLong(scopeKey);

    variablesColumnFamily.whileEqualPrefix(
        this.scopeKey,
        (compositeKey, variable) -> {
          final DbString variableName = compositeKey.getSecond();

          if (variableFilter.test(variableName)) {
            variableConsumer.accept(variableName, variable);
          }

          return !completionCondition.getAsBoolean();
        });
    return false;
  }

  private long getRootScopeKey(final long scopeKey) {
    long rootScopeKey = scopeKey;
    long currentScopeKey = scopeKey;

    do {
      currentScopeKey = getParentScopeKey(currentScopeKey);
      if (currentScopeKey != NO_PARENT) {
        rootScopeKey = currentScopeKey;
      }
    } while (currentScopeKey != NO_PARENT);

    return rootScopeKey;
  }

  public interface VariableListener {

    void onCreate(
        long key,
        long workflowKey,
        DirectBuffer name,
        DirectBuffer value,
        long variableScopeKey,
        long rootScopeKey);

    void onUpdate(
        long key,
        long workflowKey,
        DirectBuffer name,
        DirectBuffer value,
        long variableScopeKey,
        long rootScopeKey);
  }
}
