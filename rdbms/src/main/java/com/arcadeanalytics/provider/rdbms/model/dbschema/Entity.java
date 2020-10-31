package com.arcadeanalytics.provider.rdbms.model.dbschema;

/*-
 * #%L
 * Arcade Connectors
 * %%
 * Copyright (C) 2018 - 2020 ArcadeData
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.arcadeanalytics.provider.DataSourceInfo;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * It represents an entity of the source DB.
 *
 * @author Gabriele Ponzi
 */

public class Entity implements Comparable<Entity> {
  private final String name;
  private final DataSourceInfo dataSource;
  private final String schemaName;
  private final Set<Attribute> attributes;
  private final List<ForeignKey> foreignKeys;

  // Canonical relationships
  private final Set<CanonicalRelationship> outCanonicalRelationships;
  private final Set<CanonicalRelationship> inCanonicalRelationships;
  // Logical relationships
  private final Set<LogicalRelationship> outLogicalRelationships;
  private final Set<LogicalRelationship> inLogicalRelationships;
  private Boolean isSplitEntity;

  //STATE!!!!
  private Set<CanonicalRelationship> inheritedInCanonicalRelationships;
  private boolean inheritedInCanonicalRelationshipsRecovered;
  private Set<CanonicalRelationship> inheritedOutCanonicalRelationships;
  private boolean inheritedOutCanonicalRelationshipsRecovered;
  //FIXME: all Boolean or boolean, please
  private Boolean isAggregable;
  private Set<Attribute> inheritedAttributes;
  private boolean inheritedAttributesRecovered;
  private int schemaPosition;
  private PrimaryKey primaryKey;
  private String directionOfN2NRepresentedRelationship; // when the entity corresponds to an aggregable
  // join table it's 'direct' by default (at the
  // first invocation of 'isAggregableJoinTable()')
  private String nameOfN2NRepresentedRelationship; // we can have this parameter only in a join table
  // and with the manual migrationConfigDoc of its
  // represented relationship
  private Entity parentEntity;
  private int inheritanceLevel;
  private HierarchicalBag hierarchicalBag;

  public Entity(String name, String schemaName, DataSourceInfo dataSource) {
    this.name = name;
    this.dataSource = dataSource;
    this.schemaName = schemaName;
    this.attributes = new LinkedHashSet<>();
    this.inheritedAttributes = new LinkedHashSet<>();
    this.inheritedAttributesRecovered = false;
    this.foreignKeys = new LinkedList<>();

    // canonical relationships
    this.outCanonicalRelationships = new LinkedHashSet<>();
    this.inheritedOutCanonicalRelationships = new LinkedHashSet<>();
    this.inheritedOutCanonicalRelationshipsRecovered = false;
    this.inCanonicalRelationships = new LinkedHashSet<>();
    this.inheritedInCanonicalRelationships = new LinkedHashSet<>();
    this.inheritedInCanonicalRelationshipsRecovered = false;

    // logical relationships
    this.outLogicalRelationships = new LinkedHashSet<>();
    this.inLogicalRelationships = new LinkedHashSet<>();

    this.isAggregable = null;
    this.isSplitEntity = false;
    this.inheritanceLevel = 0;
  }

  /*
   * It's possible to aggregate an entity iff (i) It's a junction (or join) table of dimension 2. (ii) It has not exported keys,
   * that is it's not referenced by other entities.
   */
  public boolean isAggregableJoinTable() {
    // if already known, just retrieve the info
    if (isAggregable != null) {
      return isAggregable;
    } else {
      // (i) preliminar check
      if (foreignKeys.size() != 2) {
        return false;
      } else {
        boolean aggregable = isJunctionTable();
        isAggregable = aggregable;

        // if the entity is an aggregable join table then the direction of the N-N represented relationship is set to 'direct' by
        // default.
        if (isAggregable && directionOfN2NRepresentedRelationship == null) {
          directionOfN2NRepresentedRelationship = "direct";
        }

        return isAggregable;
      }
    }
  }

  private boolean isJunctionTable() {
    boolean isJunctionTable = true;

    // (i) it's a junction table iff each attribute belonging to the primary key is involved also in a foreign key that imports all
    // the attributes of the primary key of the referenced table.
    for (ForeignKey currentFk : this.foreignKeys) {
      for (Attribute attribute : currentFk.getInvolvedAttributes()) {
        if (!this.primaryKey.getInvolvedAttributes().contains(attribute)) {
          isJunctionTable = false;
          break;
        }
      }
    }

    // (ii) check
    if (isJunctionTable) {
      if (this.getAllInCanonicalRelationships().size() > 0) isJunctionTable = false;
    }
    return isJunctionTable;
  }

  public Boolean isSplitEntity() {
    return this.isSplitEntity;
  }

  public void setIsSplitEntity(Boolean splitEntity) {
    isSplitEntity = splitEntity;
  }

  public void setIsAggregableJoinTable(boolean isAggregable) {
    this.isAggregable = isAggregable;
  }

  public String getDirectionOfN2NRepresentedRelationship() {
    return this.directionOfN2NRepresentedRelationship;
  }

  public void setDirectionOfN2NRepresentedRelationship(String directionOfN2NRepresentedRelationship) {
    this.directionOfN2NRepresentedRelationship = directionOfN2NRepresentedRelationship;
  }

  public String getNameOfN2NRepresentedRelationship() {
    return this.nameOfN2NRepresentedRelationship;
  }

  public void setNameOfN2NRepresentedRelationship(String nameOfN2NRepresentedRelationship) {
    this.nameOfN2NRepresentedRelationship = nameOfN2NRepresentedRelationship;
  }

  public String getName() {
    return this.name;
  }

  public DataSourceInfo getDataSource() {
    return dataSource;
  }

  public String getSchemaName() {
    return this.schemaName;
  }

  public int getSchemaPosition() {
    return this.schemaPosition;
  }

  public void setSchemaPosition(int schemaPosition) {
    this.schemaPosition = schemaPosition;
  }

  public Set<Attribute> getAttributes() {
    return this.attributes;
  }

  public Set<Attribute> getInheritedAttributes() {
    if (inheritedAttributesRecovered) return this.inheritedAttributes; else if (parentEntity != null) {
      this.inheritedAttributes = parentEntity.getAllAttributes();
      this.inheritedAttributesRecovered = true;
      return this.inheritedAttributes;
    } else return this.inheritedAttributes;
  }

  // Returns attributes and inherited attributes
  public Set<Attribute> getAllAttributes() {
    Set<Attribute> allAttributes = new LinkedHashSet<Attribute>();
    allAttributes.addAll(this.getInheritedAttributes());
    allAttributes.addAll(this.attributes);

    return allAttributes;
  }

  public boolean isInheritedAttributesRecovered() {
    return inheritedAttributesRecovered;
  }

  public PrimaryKey getPrimaryKey() {
    return this.primaryKey;
  }

  public void setPrimaryKey(PrimaryKey primaryKey) {
    this.primaryKey = primaryKey;
  }

  public List<ForeignKey> getForeignKeys() {
    return foreignKeys;
  }

  public boolean addAttribute(Attribute attribute) {
    boolean added = this.attributes.add(attribute);
    List<Attribute> temp = new LinkedList<Attribute>(this.attributes);

    if (added) {
      Collections.sort(temp);
    }
    this.attributes.clear();
    this.attributes.addAll(temp);
    return added;
  }

  public void removeAttributeByNameIgnoreCase(String toRemove) {
    Attribute currentAttribute;
    Iterator<Attribute> it = this.attributes.iterator();
    while (it.hasNext()) {
      currentAttribute = it.next();
      if (currentAttribute.getName().equalsIgnoreCase(toRemove)) {
        it.remove();
        break;
      }
    }
  }

  public Attribute getAttributeByName(String name) {
    Attribute toReturn = null;

    for (Attribute a : this.attributes) {
      if (a.getName().equals(name)) {
        toReturn = a;
        break;
      }
    }
    return toReturn;
  }

  public Attribute getAttributeByNameIgnoreCase(String name) {
    Attribute toReturn = null;

    for (Attribute a : this.attributes) {
      if (a.getName().equalsIgnoreCase(name)) {
        toReturn = a;
        break;
      }
    }
    return toReturn;
  }

  public Attribute getAttributeByOrdinalPosition(int position) {
    Attribute toReturn = null;

    for (Attribute a : this.attributes) {
      if (a.getOrdinalPosition() == position) {
        toReturn = a;
        break;
      }
    }

    return toReturn;
  }

  public Attribute getInheritedAttributeByName(String name) {
    Attribute toReturn = null;

    for (Attribute a : this.getInheritedAttributes()) {
      if (a.getName().equals(name)) {
        toReturn = a;
        break;
      }
    }

    return toReturn;
  }

  public Attribute getInheritedAttributeByNameIgnoreCase(String name) {
    Attribute toReturn = null;

    for (Attribute a : this.getInheritedAttributes()) {
      if (a.getName().equalsIgnoreCase(name)) {
        toReturn = a;
        break;
      }
    }

    return toReturn;
  }

  // Getter and Setter Out Relationships

  public Set<CanonicalRelationship> getOutCanonicalRelationships() {
    return this.outCanonicalRelationships;
  }

  public Set<CanonicalRelationship> getInheritedOutCanonicalRelationships() {
    if (inheritedOutCanonicalRelationshipsRecovered) return this.inheritedOutCanonicalRelationships; else if (parentEntity != null) {
      this.inheritedOutCanonicalRelationships = parentEntity.getAllOutCanonicalRelationships();
      this.inheritedOutCanonicalRelationshipsRecovered = true;
      return this.inheritedOutCanonicalRelationships;
    } else return this.inheritedOutCanonicalRelationships;
  }

  // Returns relationships and inherited relationships (OUT)
  public Set<CanonicalRelationship> getAllOutCanonicalRelationships() {
    Set<CanonicalRelationship> allRelationships = new LinkedHashSet<CanonicalRelationship>();
    allRelationships.addAll(this.getInheritedOutCanonicalRelationships());
    allRelationships.addAll(this.outCanonicalRelationships);

    return allRelationships;
  }

  public boolean isInheritedOutCanonicalRelationshipsRecovered() {
    return inheritedOutCanonicalRelationshipsRecovered;
  }

  public void setInheritedOutCanonicalRelationshipsRecovered(boolean inheritedOutCanonicalRelationshipsRecovered) {
    this.inheritedOutCanonicalRelationshipsRecovered = inheritedOutCanonicalRelationshipsRecovered;
  }

  // Getter and Setter In Relationships

  public Set<CanonicalRelationship> getInCanonicalRelationships() {
    return this.inCanonicalRelationships;
  }

  public Set<CanonicalRelationship> getInheritedInCanonicalRelationships() {
    if (inheritedInCanonicalRelationshipsRecovered) return this.inheritedInCanonicalRelationships; else if (parentEntity != null) {
      this.inheritedInCanonicalRelationships = parentEntity.getAllInCanonicalRelationships();
      this.inheritedInCanonicalRelationshipsRecovered = true;
      return this.inheritedInCanonicalRelationships;
    } else return this.inheritedInCanonicalRelationships;
  }

  public void setInheritedInCanonicalRelationships(Set<CanonicalRelationship> inheritedInCanonicalRelationships) {
    this.inheritedInCanonicalRelationships = inheritedInCanonicalRelationships;
  }

  // Returns relationships and inherited relationships (IN)
  public Set<CanonicalRelationship> getAllInCanonicalRelationships() {
    Set<CanonicalRelationship> allRelationships = new LinkedHashSet<CanonicalRelationship>();
    allRelationships.addAll(this.getInheritedInCanonicalRelationships());
    allRelationships.addAll(this.inCanonicalRelationships);

    return allRelationships;
  }

  public boolean isInheritedInCanonicalRelationshipsRecovered() {
    return inheritedInCanonicalRelationshipsRecovered;
  }

  public Set<LogicalRelationship> getOutLogicalRelationships() {
    return outLogicalRelationships;
  }

  public Set<LogicalRelationship> getInLogicalRelationships() {
    return inLogicalRelationships;
  }

  public Entity getParentEntity() {
    return this.parentEntity;
  }

  public void setParentEntity(Entity parentEntity) {
    this.parentEntity = parentEntity;
  }

  public int getInheritanceLevel() {
    return this.inheritanceLevel;
  }

  public void setInheritanceLevel(int inheritanceLevel) {
    this.inheritanceLevel = inheritanceLevel;
  }

  public HierarchicalBag getHierarchicalBag() {
    return hierarchicalBag;
  }

  public void setHierarchicalBag(HierarchicalBag hierarchicalBag) {
    this.hierarchicalBag = hierarchicalBag;
  }

  public void renumberAttributesOrdinalPositions() {
    int i = 1;
    for (Attribute attribute : this.attributes) {
      attribute.setOrdinalPosition(i);
      i++;
    }
  }

  @Override
  public int compareTo(Entity toCompare) {
    if (this.inheritanceLevel > toCompare.getInheritanceLevel()) return 1; else if (
      this.inheritanceLevel < toCompare.getInheritanceLevel()
    ) return -1; else return this.name.compareTo(toCompare.getName());
  }

  @Override
  public String toString() {
    String s = "Entity [name = " + this.name + ", number of attributes = " + this.attributes.size() + "]";

    if (this.isAggregableJoinTable()) s += "\t\t\tJoin Entity (Aggregable Join Table)";

    s += "\n|| ";

    for (Attribute a : this.attributes) s += a.getOrdinalPosition() + ": " + a.getName() + " ( " + a.getDataType() + " ) || ";

    s += "\nPrimary Key (" + this.primaryKey.getInvolvedAttributes().size() + " involved attributes): ";

    int cont = 1;
    int size = this.primaryKey.getInvolvedAttributes().size();
    for (Attribute a : this.primaryKey.getInvolvedAttributes()) {
      if (cont < size) s += a.getName() + ", "; else s += a.getName() + ".";
      cont++;
    }

    if (this.outCanonicalRelationships.size() > 0) {
      s += "\nForeign Keys (" + outCanonicalRelationships.size() + "):\n";
      int index = 1;

      for (CanonicalRelationship relationship : this.outCanonicalRelationships) {
        s += index + ".  ";
        s +=
          "Foreign Entity: " +
          relationship.getForeignEntity().getName() +
          ", Foreign Key: " +
          relationship.getForeignKey().toString() +
          "\t||\t" +
          "Parent Entity: " +
          relationship.getParentEntity().getName() +
          ", Primary Key: " +
          relationship.getForeignKey().toString() +
          "\n";
        index++;
      }
    } else {
      s += "\nForeign Key: Not Present\n";
    }

    s += "\n\n";
    return s;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    Entity that = (Entity) obj;
    return this.name.equals(that.getName()) && this.getDataSource().equals(that.getDataSource());
  }
}
