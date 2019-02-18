/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cache

import (
	"context"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// NewCacheFunc - Function for creating a new cache from the options and a rest config
type NewCacheFunc func(config *rest.Config, opts Options) (Cache, error)

// MultiNamespacedCacheBuilder - Builder function to create a new multi-namespaced cache.
// This will scope the cache to a list of namespaces. Listing for all namespaces
// will list for all the namespaces that this knows about.
func MultiNamespacedCacheBuilder(namespaces []string) NewCacheFunc {
	return func(config *rest.Config, opts Options) (Cache, error) {
		opts, err := defaultOpts(config, opts)
		if err != nil {
			return nil, err
		}
		caches := map[string]Cache{}
		for _, ns := range namespaces {
			opts.Namespace = ns
			c, err := New(config, opts)
			if err != nil {
				return nil, err
			}
			caches[ns] = c
		}
		return &multiNamespaceCache{namespaceToCache: caches, Scheme: opts.Scheme}, nil
	}
}

// multiNamespaceCache knows how to handle multiple namespaced caches
// Use this feature when scoping permissions for your
// operator to a list of namespaces instead of watching every namespace
// in the cluster.
type multiNamespaceCache struct {
	namespaceToCache map[string]Cache
	Scheme           *runtime.Scheme
}

var _ Cache = &multiNamespaceCache{}

// Methods for multiNamespaceCache to conform to the Informers interface
func (c *multiNamespaceCache) GetInformer(obj runtime.Object) (Informer, error) {
	informers := map[string]Informer{}
	for ns, cache := range c.namespaceToCache {
		informer, err := cache.GetInformer(obj)
		if err != nil {
			return nil, err
		}
		informers[ns] = informer
	}
	return &multiNamespaceInformer{namespaceToInformer: informers}, nil
}

func (c *multiNamespaceCache) GetInformerForKind(gvk schema.GroupVersionKind) (Informer, error) {
	informers := map[string]Informer{}
	for ns, cache := range c.namespaceToCache {
		informer, err := cache.GetInformerForKind(gvk)
		if err != nil {
			return nil, err
		}
		informers[ns] = informer
	}
	return &multiNamespaceInformer{namespaceToInformer: informers}, nil
}

func (c *multiNamespaceCache) Start(stopCh <-chan struct{}) error {
	for ns, cache := range c.namespaceToCache {
		go func(ns string, cache Cache) {
			err := cache.Start(stopCh)
			if err != nil {
				log.Error(err, "multinamespace cache failed to start namespaced informer", "namespace", ns)
			}
		}(ns, cache)
	}
	<-stopCh
	return nil
}

func (c *multiNamespaceCache) WaitForCacheSync(stop <-chan struct{}) bool {
	synced := true
	for _, cache := range c.namespaceToCache {
		if s := cache.WaitForCacheSync(stop); !s {
			synced = s
		}
	}
	return synced
}

func (c *multiNamespaceCache) IndexField(obj runtime.Object, field string, extractValue client.IndexerFunc) error {
	for _, cache := range c.namespaceToCache {
		if err := cache.IndexField(obj, field, extractValue); err != nil {
			return err
		}
	}
	return nil
}

func (c *multiNamespaceCache) Get(ctx context.Context, key client.ObjectKey, obj runtime.Object) error {
	cache, ok := c.namespaceToCache[key.Namespace]
	if !ok {
		return fmt.Errorf("unable to get: %v because of unknown namespace for the cache", key)
	}
	return cache.Get(ctx, key, obj)
}

// List multi namespace cache will get all the objects in the namespaces that the cache is watching if asked for all namespaces.
func (c *multiNamespaceCache) List(ctx context.Context, list runtime.Object, opts ...client.ListOptionFunc) error {
	listOpts := client.ListOptions{}
	listOpts.ApplyOptions(opts)
	if listOpts.Namespace != corev1.NamespaceAll {
		cache, ok := c.namespaceToCache[listOpts.Namespace]
		if !ok {
			return fmt.Errorf("unable to get: %v because of unknown namespace for the cache", listOpts.Namespace)
		}
		return cache.List(ctx, list, opts...)
	}

	// Get all the objects in the namespaces we are watching.
	gvk, err := apiutil.GVKForObject(list, c.Scheme)
	if err != nil {
		return err
	}

	vAllItems := reflect.ValueOf(list).Elem().FieldByName("Items")
	vResourceVersion := reflect.ValueOf(list).Elem().FieldByName("ResourceVersion")
	for _, cache := range c.namespaceToCache {
		items, err := c.Scheme.New(gvk)
		if err != nil {
			return err
		}
		err = cache.List(ctx, items, opts...)
		if err != nil {
			return err
		}
		elems := reflect.ValueOf(items).Elem()
		vItems := elems.FieldByName("Items")
		vRV := elems.FieldByName("ResourceVersion")
		vAllItems = reflect.AppendSlice(vAllItems, vItems)
		// The last list call should have the most correct resource version.
		vResourceVersion.SetString(vRV.String())
	}
	reflect.ValueOf(list).Elem().FieldByName("Items").Set(vAllItems)
	reflect.ValueOf(list).Elem().FieldByName("ResourceVersion").Set(vResourceVersion)

	return nil
}

// multiNamespaceInformer knows how to handle interacting with the underlying informer across multiple namespaces
type multiNamespaceInformer struct {
	namespaceToInformer map[string]Informer
}

var _ Informer = &multiNamespaceInformer{}

// AddEventHandler adds the handler to each namespaced informer
func (i *multiNamespaceInformer) AddEventHandler(handler toolscache.ResourceEventHandler) {
	for _, informer := range i.namespaceToInformer {
		informer.AddEventHandler(handler)
	}
}

// AddEventHandlerWithResyncPeriod adds the handler with a resync period to each namespaced informer
func (i *multiNamespaceInformer) AddEventHandlerWithResyncPeriod(handler toolscache.ResourceEventHandler, resyncPeriod time.Duration) {
	for _, informer := range i.namespaceToInformer {
		informer.AddEventHandlerWithResyncPeriod(handler, resyncPeriod)
	}
}

// AddIndexers adds the indexer for each namespaced informer
func (i *multiNamespaceInformer) AddIndexers(indexers toolscache.Indexers) error {
	for _, informer := range i.namespaceToInformer {
		err := informer.AddIndexers(indexers)
		if err != nil {
			return err
		}
	}
	return nil
}

// HasSynced checks if each namespaced informer has synced
func (i *multiNamespaceInformer) HasSynced() bool {
	for _, informer := range i.namespaceToInformer {
		if ok := informer.HasSynced(); !ok {
			return ok
		}
	}
	return true
}
