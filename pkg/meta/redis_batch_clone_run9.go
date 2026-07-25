//go:build !noredis

/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisBatchCloneEntry struct {
	srcIno Ino
	dstIno Ino
	name   string
}

type redisBatchCloneChunk struct {
	index  uint32
	slices []string
	refs   []*slice
}

type redisBatchCloneSource struct {
	attr    Attr
	xattrs  map[string]string
	chunks  []redisBatchCloneChunk
	symlink string
}

// doBatchClone clones all non-directory entries with one Redis transaction.
//
// Redis WATCH protects the source metadata and destination directory while
// pipelined reads collapse metadata fetches into two round trips. The final
// MULTI/EXEC publishes every destination entry atomically. This is important
// for run9, where a snap fork clones a Badger metadata directory and must not
// fan one Redis transaction out for every file.
func (m *redisMeta) doBatchClone(ctx Context, _ Ino, dstParent Ino, entries []*Entry, cmode uint8, cumask uint16, result *batchCloneResult) syscall.Errno {
	if len(entries) == 0 {
		return 0
	}

	cloneEntries := make([]redisBatchCloneEntry, len(entries))
	sourceInodes := make([]Ino, 0, len(entries))
	sourceSeen := make(map[Ino]struct{}, len(entries))
	nameSeen := make(map[string]struct{}, len(entries))
	watchKeys := []string{m.inodeKey(dstParent), m.entryKey(dstParent)}
	for i, entry := range entries {
		name := string(entry.Name)
		if _, exists := nameSeen[name]; exists {
			return syscall.EEXIST
		}
		nameSeen[name] = struct{}{}

		dstIno, err := m.nextInode()
		if err != nil {
			return errno(err)
		}
		cloneEntries[i] = redisBatchCloneEntry{
			srcIno: entry.Inode,
			dstIno: dstIno,
			name:   name,
		}
		if _, exists := sourceSeen[entry.Inode]; !exists {
			sourceSeen[entry.Inode] = struct{}{}
			sourceInodes = append(sourceInodes, entry.Inode)
			watchKeys = append(watchKeys, m.inodeKey(entry.Inode), m.xattrKey(entry.Inode))
		}
	}

	var committedResult batchCloneResult
	err := m.txn(ctx, func(tx *redis.Tx) error {
		inodeKeys := make([]string, 0, len(sourceInodes)+1)
		inodeKeys = append(inodeKeys, m.inodeKey(dstParent))
		for _, ino := range sourceInodes {
			inodeKeys = append(inodeKeys, m.inodeKey(ino))
		}

		entryNames := make([]string, len(cloneEntries))
		for i := range cloneEntries {
			entryNames[i] = cloneEntries[i].name
		}

		var inodeValues *redis.SliceCmd
		var destinationValues *redis.SliceCmd
		xattrValues := make(map[Ino]*redis.MapStringStringCmd, len(sourceInodes))
		if _, err := tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			inodeValues = pipe.MGet(ctx, inodeKeys...)
			destinationValues = pipe.HMGet(ctx, m.entryKey(dstParent), entryNames...)
			for _, ino := range sourceInodes {
				xattrValues[ino] = pipe.HGetAll(ctx, m.xattrKey(ino))
			}
			return nil
		}); err != nil {
			return err
		}

		values, err := inodeValues.Result()
		if err != nil {
			return err
		}
		if values[0] == nil {
			return syscall.ENOENT
		}
		var parentAttr Attr
		m.parseAttr([]byte(values[0].(string)), &parentAttr)
		if parentAttr.Typ != TypeDirectory {
			return syscall.ENOTDIR
		}
		if (parentAttr.Flags & FlagImmutable) != 0 {
			return syscall.EPERM
		}
		if st := m.Access(ctx, dstParent, MODE_MASK_W|MODE_MASK_X, &parentAttr); st != 0 {
			return st
		}

		destinations, err := destinationValues.Result()
		if err != nil {
			return err
		}
		for _, destination := range destinations {
			if destination != nil {
				return syscall.EEXIST
			}
		}

		sources := make(map[Ino]*redisBatchCloneSource, len(sourceInodes))
		dataWatchKeys := make([]string, 0, len(sourceInodes))
		for i, ino := range sourceInodes {
			if values[i+1] == nil {
				return syscall.ENOENT
			}
			source := &redisBatchCloneSource{}
			m.parseAttr([]byte(values[i+1].(string)), &source.attr)
			if source.attr.Typ == TypeDirectory {
				return syscall.EINVAL
			}
			if st := m.Access(ctx, ino, MODE_MASK_R, &source.attr); st != 0 {
				return st
			}
			source.xattrs, err = xattrValues[ino].Result()
			if err != nil {
				return err
			}
			sources[ino] = source

			switch source.attr.Typ {
			case TypeFile:
				if source.attr.Length > 0 {
					for index := uint32(0); index <= uint32(source.attr.Length/ChunkSize); index++ {
						dataWatchKeys = append(dataWatchKeys, m.chunkKey(ino, index))
					}
				}
			case TypeSymlink:
				dataWatchKeys = append(dataWatchKeys, m.symKey(ino))
			}
		}
		if len(dataWatchKeys) > 0 {
			if err := tx.Watch(ctx, dataWatchKeys...).Err(); err != nil {
				return err
			}
		}

		chunkValues := make(map[Ino][]*redis.StringSliceCmd, len(sourceInodes))
		symlinkValues := make(map[Ino]*redis.StringCmd, len(sourceInodes))
		if _, err := tx.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, ino := range sourceInodes {
				source := sources[ino]
				switch source.attr.Typ {
				case TypeFile:
					if source.attr.Length > 0 {
						commands := make([]*redis.StringSliceCmd, 0, source.attr.Length/ChunkSize+1)
						for index := uint32(0); index <= uint32(source.attr.Length/ChunkSize); index++ {
							commands = append(commands, pipe.LRange(ctx, m.chunkKey(ino, index), 0, -1))
						}
						chunkValues[ino] = commands
					}
				case TypeSymlink:
					symlinkValues[ino] = pipe.Get(ctx, m.symKey(ino))
				}
			}
			return nil
		}); err != nil {
			return err
		}

		for ino, commands := range chunkValues {
			source := sources[ino]
			for index, command := range commands {
				slices, err := command.Result()
				if err != nil {
					return err
				}
				if len(slices) == 0 {
					continue
				}
				refs := readSlices(slices)
				if refs == nil {
					return syscall.EIO
				}
				source.chunks = append(source.chunks, redisBatchCloneChunk{
					index:  uint32(index),
					slices: slices,
					refs:   refs,
				})
			}
		}
		for ino, command := range symlinkValues {
			target, err := command.Result()
			if err != nil {
				return err
			}
			sources[ino].symlink = target
		}

		now := time.Now()
		nextResult := batchCloneResult{
			userGroupQuotas: make([]userGroupQuotaDelta, 0, len(cloneEntries)),
		}
		inodeUpdates := make(map[string]interface{}, len(cloneEntries)+1)
		destinationUpdates := make(map[string]interface{}, len(cloneEntries))
		sliceRefUpdates := make(map[string]int64)
		for _, entry := range cloneEntries {
			attr := sources[entry.srcIno].attr
			attr.Parent = dstParent
			if cmode&CLONE_MODE_PRESERVE_ATTR == 0 {
				attr.Uid = ctx.Uid()
				attr.Gid = ctx.Gid()
				attr.Mode &= ^cumask
				attr.Atime = now.Unix()
				attr.Mtime = now.Unix()
				attr.Ctime = now.Unix()
				attr.Atimensec = uint32(now.Nanosecond())
				attr.Mtimensec = uint32(now.Nanosecond())
				attr.Ctimensec = uint32(now.Nanosecond())
			}
			if attr.Typ == TypeFile && attr.Nlink > 1 {
				attr.Nlink = 1
			}
			inodeUpdates[m.inodeKey(entry.dstIno)] = m.marshal(&attr)
			destinationUpdates[entry.name] = m.packEntry(attr.Typ, entry.dstIno)

			entrySpace := align4K(attr.Length)
			nextResult.length += int64(attr.Length)
			nextResult.space += entrySpace
			nextResult.inodes++
			nextResult.userGroupQuotas = append(nextResult.userGroupQuotas, userGroupQuotaDelta{
				Uid: attr.Uid, Gid: attr.Gid, Space: entrySpace, Inodes: 1,
			})

			for _, chunk := range sources[entry.srcIno].chunks {
				for _, ref := range chunk.refs {
					if ref.id > 0 {
						sliceRefUpdates[m.sliceKey(ref.id, ref.size)]++
					}
				}
			}
		}
		if cmode&CLONE_MODE_PRESERVE_ATTR == 0 {
			parentAttr.Mtime = now.Unix()
			parentAttr.Mtimensec = uint32(now.Nanosecond())
			parentAttr.Ctime = now.Unix()
			parentAttr.Ctimensec = uint32(now.Nanosecond())
			inodeUpdates[m.inodeKey(dstParent)] = m.marshal(&parentAttr)
		}

		if _, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.MSet(ctx, inodeUpdates)
			pipe.HSet(ctx, m.entryKey(dstParent), destinationUpdates)
			pipe.IncrBy(ctx, m.usedSpaceKey(), nextResult.space)
			pipe.IncrBy(ctx, m.totalInodesKey(), nextResult.inodes)
			for _, entry := range cloneEntries {
				source := sources[entry.srcIno]
				if len(source.xattrs) > 0 {
					pipe.HSet(ctx, m.xattrKey(entry.dstIno), source.xattrs)
				}
				for _, chunk := range source.chunks {
					pipe.RPush(ctx, m.chunkKey(entry.dstIno, chunk.index), chunk.slices)
				}
				if source.attr.Typ == TypeSymlink {
					pipe.Set(ctx, m.symKey(entry.dstIno), source.symlink, 0)
				}
			}
			for key, delta := range sliceRefUpdates {
				pipe.HIncrBy(ctx, m.sliceRefs(), key, delta)
			}
			return nil
		}); err != nil {
			return err
		}
		committedResult = nextResult
		return nil
	}, watchKeys...)
	if err != nil {
		return errno(err)
	}
	*result = committedResult
	return 0
}
