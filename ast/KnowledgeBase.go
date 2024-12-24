//  Copyright hyperjumptech/grule-rule-engine Authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package ast

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/hyperjumptech/grule-rule-engine/pkg"

	"github.com/duke-git/lancet/v2/slice"
)

// NewKnowledgeLibrary create a new instance KnowledgeLibrary
func NewKnowledgeLibrary() *KnowledgeLibrary {

	return &KnowledgeLibrary{
		Library: make(map[string]*KnowledgeBase),
	}
}

// KnowledgeLibrary is a knowledgebase store.
type KnowledgeLibrary struct {
	Library map[string]*KnowledgeBase
}

// GetKnowledgeBase will get the actual KnowledgeBase blue print that will be used to create instances.
// Although this KnowledgeBase blueprint works, It SHOULD NOT be used directly in the engine.
// You should obtain KnowledgeBase instance by calling NewKnowledgeBaseInstance
func (lib *KnowledgeLibrary) GetKnowledgeBase(name, version string) *KnowledgeBase {
	knowledgeBase, ok := lib.Library[fmt.Sprintf("%s:%s", name, version)]
	if ok {

		return knowledgeBase
	}
	knowledgeBase = &KnowledgeBase{
		Name:          name,
		Version:       version,
		RuleEntries:   make(map[string]*RuleEntry),
		RuleNames:     make([]string, 0),
		WorkingMemory: NewWorkingMemory(name, version),
	}
	lib.Library[fmt.Sprintf("%s:%s", name, version)] = knowledgeBase

	return knowledgeBase
}

// RemoveRuleEntry mark the rule entry as deleted
func (lib *KnowledgeLibrary) RemoveRuleEntry(ruleName, name string, version string) {
	nameVersion := fmt.Sprintf("%s:%s", name, version)
	knowledgeBase, ok := lib.Library[nameVersion]
	if ok {
		knowledgeBase.RemoveRuleEntry(ruleName)
	}
}

// LoadKnowledgeBaseFromReader will load the KnowledgeBase stored using StoreKnowledgeBaseToWriter function
// be it from file, or anywhere. The reader we needed is a plain io.Reader, thus closing the source stream is your responsibility.
// This should hopefully speedup loading huge ruleset by storing and reading them
// without having to parse the GRL.
func (lib *KnowledgeLibrary) LoadKnowledgeBaseFromReader(reader io.Reader, overwrite bool) (retKb *KnowledgeBase, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retKb = nil
			retErr = fmt.Errorf("panic recovered during LoadKnowledgeBaseFromReader, recover \"%v\". send us your report to https://github.com/hyperjumptech/grule-rule-engine/issues", r)
		}
	}()

	catalog := &Catalog{}
	err := catalog.ReadCatalogFromReader(reader)
	if err != nil && err != io.EOF {

		return nil, err
	}
	knowledgeBase, err := catalog.BuildKnowledgeBase()
	if err != nil {
		return nil, err
	}
	if overwrite {
		lib.Library[fmt.Sprintf("%s:%s", knowledgeBase.Name, knowledgeBase.Version)] = knowledgeBase

		return knowledgeBase, nil
	}
	if _, ok := lib.Library[fmt.Sprintf("%s:%s", knowledgeBase.Name, knowledgeBase.Version)]; !ok {
		lib.Library[fmt.Sprintf("%s:%s", knowledgeBase.Name, knowledgeBase.Version)] = knowledgeBase

		return knowledgeBase, nil
	}

	return nil, fmt.Errorf("KnowledgeBase %s version %s exist", knowledgeBase.Name, knowledgeBase.Version)
}

// StoreKnowledgeBaseToWriter will store a KnowledgeBase in binary form
// once store, the binary stream can be read using LoadKnowledgeBaseFromReader function.
// This should hopefully speedup loading huge ruleset by storing and reading them
// without having to parse the GRL.
//
// The stored binary file is greatly increased (easily 10x fold) due to lots of generated keys for AST Nodes
// that was also saved. To overcome this, the use of archive/zip package for Readers and Writers could cut down the
// binary size quite a lot.
func (lib *KnowledgeLibrary) StoreKnowledgeBaseToWriter(writer io.Writer, name, version string) error {
	kb := lib.GetKnowledgeBase(name, version)
	cat := kb.MakeCatalog()
	err := cat.WriteCatalogToWriter(writer)

	return err
}

// NewKnowledgeBaseInstance will create a new instance based on KnowledgeBase blue print
// identified by its name and version
func (lib *KnowledgeLibrary) NewKnowledgeBaseInstance(name, version string) (*KnowledgeBase, error) {
	knowledgeBase, ok := lib.Library[fmt.Sprintf("%s:%s", name, version)]
	if ok {
		newClone, err := knowledgeBase.Clone(pkg.NewCloneTable())
		if err != nil {
			return nil, err
		}
		if knowledgeBase.IsIdentical(newClone) {
			AstLog.Debugf("Successfully create instance [%s:%s]", newClone.Name, newClone.Version)

			return newClone, nil
		}
		AstLog.Fatalf("ORIGIN   : %s", knowledgeBase.GetSnapshot())
		AstLog.Fatalf("CLONE    : %s", newClone.GetSnapshot())

		return nil, fmt.Errorf("the clone is not identical")
	}

	return nil, fmt.Errorf("specified knowledge base name and version not exist")
}

// KnowledgeBase is a collection of RuleEntries. It has a name and version.
type KnowledgeBase struct {
	lock          sync.Mutex
	Name          string
	Version       string
	DataContext   IDataContext
	WorkingMemory *WorkingMemory
	RuleEntries   map[string]*RuleEntry
	RuleNames     []string
}

// MakeCatalog will create a catalog entry for all AST Nodes under the KnowledgeBase
// the catalog can be used to save the knowledge base into a Writer, or to
// rebuild the KnowledgeBase from it.
// This function also will catalog the WorkingMemory.
func (e *KnowledgeBase) MakeCatalog() *Catalog {
	catalog := &Catalog{
		KnowledgeBaseName:               e.Name,
		KnowledgeBaseVersion:            e.Version,
		Data:                            nil,
		MemoryName:                      "",
		MemoryVersion:                   "",
		MemoryVariableSnapshotMap:       nil,
		MemoryExpressionSnapshotMap:     nil,
		MemoryExpressionAtomSnapshotMap: nil,
		MemoryExpressionVariableMap:     nil,
		MemoryExpressionAtomVariableMap: nil,
	}
	for _, v := range e.RuleEntries {
		v.MakeCatalog(catalog)
	}
	e.WorkingMemory.MakeCatalog(catalog)

	return catalog
}

// IsIdentical will validate if two KnoledgeBase is identical. Used to validate if the origin and clone is identical.
func (e *KnowledgeBase) IsIdentical(that *KnowledgeBase) bool {
	// fmt.Printf("%s\n%s\n", e.GetSnapshot(), that.GetSnapshot())

	return e.GetSnapshot() == that.GetSnapshot()
}

// GetSnapshot will create this knowledge base signature
func (e *KnowledgeBase) GetSnapshot() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("%s:%s[", e.Name, e.Version))
	for i, name := range e.RuleNames {
		if i > 0 {
			buffer.WriteString(",")
		}
		buffer.WriteString(e.RuleEntries[name].GetSnapshot())
	}

	buffer.WriteString("]")

	return buffer.String()
}

// Clone will clone this instance of KnowledgeBase and produce another (structure wise) identical instance.
func (e *KnowledgeBase) Clone(cloneTable *pkg.CloneTable) (*KnowledgeBase, error) {
	clone := &KnowledgeBase{
		Name:        e.Name,
		Version:     e.Version,
		RuleEntries: make(map[string]*RuleEntry),
		RuleNames:   make([]string, 0),
	}
	if e.RuleEntries != nil {
		clone.RuleNames = e.RuleNames
		for _, ruleName := range e.RuleNames {
			entry := e.RuleEntries[ruleName]
			if cloneTable.IsCloned(entry.AstID) {
				clone.RuleEntries[ruleName] = cloneTable.Records[entry.AstID].CloneInstance.(*RuleEntry)
			} else {
				cloned := entry.Clone(cloneTable)
				clone.RuleEntries[ruleName] = cloned
				cloneTable.MarkCloned(entry.AstID, cloned.AstID, entry, cloned)
			}
		}
	}
	if e.WorkingMemory != nil {
		wm, err := e.WorkingMemory.Clone(cloneTable)
		if err != nil {
			return nil, err
		}
		clone.WorkingMemory = wm
	}

	return clone, nil
}

// AddRuleEntry add ruleentry into this knowledge base.
// return an error if a rule entry with the same name already exist in this knowledge base.
func (e *KnowledgeBase) AddRuleEntry(entry *RuleEntry) error {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.ContainsRuleEntry(entry.RuleName) {

		return fmt.Errorf("rule entry %s already exist", entry.RuleName)
	}
	e.RuleEntries[entry.RuleName] = entry
	e.RuleNames = append(e.RuleNames, entry.RuleName)
	return nil
}

// ContainsRuleEntry will check if a rule with such name is already exist in this knowledge base.
func (e *KnowledgeBase) ContainsRuleEntry(name string) bool {
	_, ok := e.RuleEntries[name]

	return ok
}

// RemoveRuleEntry mark the rule entry as deleted
func (e *KnowledgeBase) RemoveRuleEntry(name string) {
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.ContainsRuleEntry(name) {
		delete(e.RuleEntries, name)
		e.RuleNames = slice.DeleteAt(e.RuleNames, slice.IndexOf(e.RuleNames, name))
	}
}

// InitializeContext will initialize this AST graph with data context and working memory before running rule on them.
func (e *KnowledgeBase) InitializeContext(dataCtx IDataContext) {
	e.DataContext = dataCtx
}

// RetractRule will retract the selected rule for execution on the next cycle.
func (e *KnowledgeBase) RetractRule(ruleName string) {
	re, ok := e.RuleEntries[ruleName]
	if ok {
		re.Retracted = true
	}
}

// IsRuleRetracted will check if a certain rule denoted by its rule name is currently retracted
func (e *KnowledgeBase) IsRuleRetracted(ruleName string) bool {
	re, ok := e.RuleEntries[ruleName]
	if !ok {
		return false
	}
	return re.Retracted
}

// Reset will restore all rule in the knowledge
func (e *KnowledgeBase) Reset() {
	for _, re := range e.RuleEntries {
		if re.Retracted {
			re.Retracted = false
		}
	}
}
