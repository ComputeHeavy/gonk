import fs
import core
import uuid
import sigs
import json
import sqlite
import events
import sqlite3
import hashlib
import unittest
import test_utils

import nacl
from nacl import signing

def rmtree(p):
    for ea in p.iterdir():
        if ea.is_dir():
            rmtree(ea)
        else:
            ea.unlink()
    p.rmdir()

class TestSqliteRecordKeeper(test_utils.GonkTest):
    def test_record_keeper_init(self):
        record_keeper = sqlite.RecordKeeper(
            self.test_directory)

        db_path = self.test_directory.joinpath("rk.db")
        self.assertTrue(db_path.exists())

    def test_add(self):
        record_keeper = sqlite.RecordKeeper(self.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae_in = signer.sign(events.OwnerAddEvent(bytes(sk1.verify_key).hex()))
        record_keeper.add(oae_in)

        con = sqlite3.connect(record_keeper.database_path)
        cur = con.cursor()
        cur.execute("""SELECT uuid, event FROM events""")
        res = cur.fetchone()
        con.close()
        self.assertTrue(res is not None)

        uuid_, event_json = res
        self.assertEqual(oae_in.uuid, uuid.UUID(uuid_))

        event_data = oae_in.serialize()
        event_data["type"] = oae_in.__class__.__name__
        self.assertEqual(json.dumps(event_data), event_json)

    def test_read(self):
        record_keeper = sqlite.RecordKeeper(self.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae_in = signer.sign(events.OwnerAddEvent(bytes(sk1.verify_key).hex()))
        record_keeper.add(oae_in)

        oae_out = record_keeper.read(oae_in.uuid)

        self.assertEqual(oae_in, oae_out)

    def test_exists(self):
        record_keeper = sqlite.RecordKeeper(self.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae1 = signer.sign(events.OwnerAddEvent(bytes(sk1.verify_key).hex()))

        self.assertTrue(not record_keeper.exists(oae1.uuid))

        record_keeper.add(oae1)

        self.assertTrue(record_keeper.exists(oae1.uuid))

    def test_next(self):
        record_keeper = sqlite.RecordKeeper(self.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae1 = signer.sign(events.OwnerAddEvent(bytes(sk1.verify_key).hex()))
        record_keeper.add(oae1)

        sk2 = nacl.signing.SigningKey.generate()
        oae2 = signer.sign(events.OwnerAddEvent(bytes(sk2.verify_key).hex()))
        record_keeper.add(oae2)

        self.assertEqual(record_keeper.next(), oae1.uuid)
        self.assertEqual(record_keeper.next(oae1.uuid), oae2.uuid)
        self.assertEqual(record_keeper.next(oae2.uuid), None)

    def test_tail(self):
        record_keeper = fs.RecordKeeper(self.test_directory)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae1 = signer.sign(events.OwnerAddEvent(bytes(sk1.verify_key).hex()))
        record_keeper.add(oae1)

        self.assertEqual(record_keeper.tail(), oae1.uuid)

        sk2 = nacl.signing.SigningKey.generate()
        oae2 = signer.sign(events.OwnerAddEvent(bytes(sk2.verify_key).hex()))
        record_keeper.add(oae2)

        self.assertEqual(record_keeper.tail(), oae2.uuid)

class TestSqliteState(test_utils.GonkTest):
    def standard_object(self):
        return events.Object(
            "object.txt", 
            "text/plain", 
            len("object contents"), 
            events.HashTypeT.SHA256, 
            hashlib.sha256(b"object contents").hexdigest())

    def versioned_object(self, object_: events.Object):
        obj = object_.__copy__()
        obj.version += 1
        obj.name = "object.rtf"

        return obj

    def standard_schema(self):
        return events.Object(
            "schema-sample", 
            "application/schema+json", 
            len("schema contents"), 
            events.HashTypeT.SHA256, 
            hashlib.sha256(b"schema contents").hexdigest())

    def standard_annotation(self, schema: events.Identifier):
        return events.Annotation(
            schema, 
            len("annotation contents"), 
            events.HashTypeT.SHA256, 
            hashlib.sha256(b"annotation contents").hexdigest())

    def versioned_annotation(self, annotation: events.Annotation):
        anno = annotation.__copy__()
        anno.version += 1

        return anno

    def test_machine_register(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        self.assertEqual(len(machine.validators), 2)
        self.assertEqual(machine.consumers[0], record_keeper)
        self.assertEqual(machine.validators[1], state)
        self.assertEqual(len(machine.consumers), 2)
        self.assertEqual(machine.consumers[0], record_keeper)
        self.assertEqual(machine.consumers[1], state)

    def test_object_create_accept(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT object 
                FROM objects 
                WHERE uuid = ?
                    AND version = ?""", 
            (str(o1v0.uuid), o1v0.version))

        o1v0_json = cur.fetchall()
        self.assertEqual(len(o1v0_json), 1)
        self.assertEqual(len(o1v0_json[0]), 1)

        o1v0_data = json.loads(o1v0_json[0][0])
        o1v0_out = events.Object.deserialize(o1v0_data)

        self.assertEqual(o1v0, o1v0_out)

        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(o1v0.uuid), o1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])

        self.assertTrue(events.StatusT.CREATE_PENDING in status)

        rae = events.ReviewAcceptEvent(oce.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        cur.execute("""SELECT COUNT(*)
                FROM event_review_link 
                WHERE event_uuid = ?
                    AND review_uuid = ?""", 
            (str(oce.uuid), str(rae.uuid)))

        count, = cur.fetchone()
        self.assertEqual(count, 1)

        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(o1v0.uuid), o1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])

        self.assertEqual(len(status), 0)

    def test_object_update_accept(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        o1v1 = self.versioned_object(o1v0)
        oue = signer.sign(events.ObjectUpdateEvent(o1v1))
        machine.process_event(oue)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT COUNT(*) 
                FROM objects 
                WHERE uuid = ?""", 
            (str(o1v0.uuid),))
    
        count, = cur.fetchone()
        self.assertEqual(count, 2)

        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(o1v1.uuid), o1v1.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])
        self.assertTrue(events.StatusT.CREATE_PENDING in status)

        rae = events.ReviewAcceptEvent(oue.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(o1v1.uuid), o1v1.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])
        self.assertEqual(len(status), 0)

    def test_object_delete_accept(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        ode = signer.sign(events.ObjectDeleteEvent(o1v0.identifier()))
        machine.process_event(ode)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(o1v0.uuid), o1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])
        self.assertTrue(events.StatusT.DELETE_PENDING in status)

        rae = events.ReviewAcceptEvent(ode.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(o1v0.uuid), o1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])
        self.assertTrue(events.StatusT.DELETE_ACCEPTED in status)

    def test_annotation_create_accept(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        s1v0 = self.standard_schema()
        sce = signer.sign(events.ObjectCreateEvent(s1v0))
        machine.process_event(sce)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(
            events.AnnotationCreateEvent([o1v0.identifier()], a1v0))
        machine.process_event(ace)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT annotation 
                FROM annotations 
                WHERE uuid = ?
                    AND version = ?""", 
            (str(a1v0.uuid), a1v0.version))

        a1v0_json = cur.fetchall()
        self.assertEqual(len(a1v0_json), 1)
        self.assertEqual(len(a1v0_json[0]), 1)

        a1v0_data = json.loads(a1v0_json[0][0])
        a1v0_out = events.Annotation.deserialize(a1v0_data)

        self.assertEqual(a1v0, a1v0_out)

        cur.execute("""SELECT status
                FROM annotation_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(a1v0.uuid), a1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])
        self.assertTrue(events.StatusT.CREATE_PENDING in status)

        cur.execute("""SELECT COUNT(*)
                FROM object_annotation_link
                WHERE object_uuid = ? 
                    AND object_version = ?
                    AND annotation_uuid = ?""",
            (str(o1v0.uuid), o1v0.version, str(a1v0.uuid)))

        count, = cur.fetchone()
        self.assertEqual(count, 1)

        rae = events.ReviewAcceptEvent(ace.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        cur.execute("""SELECT status
                FROM annotation_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(a1v0.uuid), a1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])
        self.assertEqual(len(status), 0)

    def test_annotation_update_accept(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))        
        machine.process_event(oce)

        s1v0 = self.standard_schema()
        sce = signer.sign(events.ObjectCreateEvent(s1v0))        
        machine.process_event(sce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(
            events.AnnotationCreateEvent([o1v0.identifier()], a1v0))            
        machine.process_event(ace)

        a1v1 = self.versioned_annotation(a1v0)
        aue = signer.sign(events.AnnotationUpdateEvent(a1v1))        
        machine.process_event(aue)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT COUNT(*)
                FROM annotations
                WHERE uuid = ?""",
            (str(a1v0.uuid),))

        count, = cur.fetchone()
        self.assertEqual(count, 2)

        cur.execute("""SELECT status
                FROM annotation_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(a1v1.uuid), a1v1.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])
        self.assertTrue(events.StatusT.CREATE_PENDING in status)

        rae = events.ReviewAcceptEvent(aue.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        cur.execute("""SELECT status
                FROM annotation_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(a1v1.uuid), a1v1.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])
        self.assertEqual(len(status), 0)

    def test_annotation_delete_accept(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))            
        machine.process_event(oce)

        s1v0 = self.standard_schema()
        sce = signer.sign(events.ObjectCreateEvent(s1v0))            
        machine.process_event(sce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(
            events.AnnotationCreateEvent([o1v0.identifier()], a1v0))
        machine.process_event(ace)

        ade = signer.sign(events.AnnotationDeleteEvent(a1v0.identifier()))    
        machine.process_event(ade)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT status
                FROM annotation_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(a1v0.uuid), a1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])
        self.assertTrue(events.StatusT.DELETE_PENDING in status)

        rae = events.ReviewAcceptEvent(ade.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        cur.execute("""SELECT status
                FROM annotation_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(a1v0.uuid), a1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])
        self.assertTrue(events.StatusT.DELETE_ACCEPTED in status)

    def test_owner_add(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer1 = sigs.Signer(sk1)
        
        vk1 = signer1.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())

        wae1 = signer1.sign(wae1)
        machine.process_event(wae1)

        if wae1.author is None:
            raise ValueError("author is none")

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT COUNT(*) 
            FROM owners 
            WHERE owner = ?""",
            (wae1.author,))
        count, = cur.fetchone()

        self.assertEqual(count, 1)

        sk2 = nacl.signing.SigningKey.generate()
        signer2 = sigs.Signer(sk2)
        oae2 = events.OwnerAddEvent(signer2.verify_bytes.hex())

        with self.assertRaises(core.ValidationError):
            oae2 = signer2.sign(oae2)
            machine.process_event(oae2)    

        oae2 = signer1.sign(oae2)
        machine.process_event(oae2)

        cur.execute("""SELECT COUNT(*) FROM owners""")
        count, = cur.fetchone()

        self.assertEqual(count, 2)

    def test_owner_remove(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer1 = sigs.Signer(sk1)
        
        vk1 = signer1.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())

        wae1 = signer1.sign(wae1)
        machine.process_event(wae1)

        sk2 = nacl.signing.SigningKey.generate()
        signer2 = sigs.Signer(sk2)
        vk2 = signer2.verify_bytes
        oae2 = events.OwnerAddEvent(vk2.hex())

        oae2 = signer1.sign(oae2)
        machine.process_event(oae2)

        with self.assertRaises(core.ValidationError):
            ore0 = signer2.sign(events.OwnerRemoveEvent(vk1.hex()))
            machine.process_event(ore0)

        ore1 = signer2.sign(events.OwnerRemoveEvent(vk2.hex()))
        machine.process_event(ore1)

        if wae1.author is None:
            raise ValueError("author is none")

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT COUNT(*) 
            FROM owners 
            WHERE owner = ?""",
            (wae1.author,))
        count, = cur.fetchone()

        self.assertEqual(count, 1)

        with self.assertRaises(core.ValidationError):
            ore2 = signer1.sign(events.OwnerRemoveEvent(vk1.hex()))
            machine.process_event(ore2)

    def test_object_create_reject(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        rre = events.ReviewRejectEvent(oce.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(o1v0.uuid), o1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])

        self.assertEqual(len(status), 1)
        self.assertTrue(events.StatusT.CREATE_REJECTED in status)

    def test_object_update_reject(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        o1v1 = self.versioned_object(o1v0)
        oue = signer.sign(events.ObjectUpdateEvent(o1v1))
        machine.process_event(oue)

        rre = events.ReviewRejectEvent(oue.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(o1v1.uuid), o1v1.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])

        self.assertEqual(len(status), 1)
        self.assertTrue(events.StatusT.CREATE_REJECTED in status)

    def test_object_delete_reject(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        ode = signer.sign(events.ObjectDeleteEvent(o1v0.identifier()))
        machine.process_event(ode)

        rre = events.ReviewRejectEvent(ode.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(o1v0.uuid), o1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])

        self.assertTrue(events.StatusT.CREATE_PENDING in status)
        self.assertEqual(len(status), 1)

    def test_annotation_create_reject(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        s1v0 = self.standard_schema()
        sce = signer.sign(events.ObjectCreateEvent(s1v0))
        machine.process_event(sce)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(
            events.AnnotationCreateEvent([o1v0.identifier()], a1v0))
        machine.process_event(ace)

        rre = events.ReviewRejectEvent(ace.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT status
                FROM annotation_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(a1v0.uuid), a1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])

        self.assertEqual(len(status), 1)
        self.assertTrue(events.StatusT.CREATE_REJECTED in status)

    def test_annotation_update_reject(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))        
        machine.process_event(oce)

        s1v0 = self.standard_schema()
        sce = signer.sign(events.ObjectCreateEvent(s1v0))        
        machine.process_event(sce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(
            events.AnnotationCreateEvent([o1v0.identifier()], a1v0))            
        machine.process_event(ace)

        a1v1 = self.versioned_annotation(a1v0)
        aue = signer.sign(events.AnnotationUpdateEvent(a1v1))        
        machine.process_event(aue)

        rre = events.ReviewRejectEvent(aue.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT status
                FROM annotation_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(a1v1.uuid), a1v1.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])

        self.assertEqual(len(status), 1)
        self.assertTrue(events.StatusT.CREATE_REJECTED in status)

    def test_annotation_delete_reject(self):
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = events.OwnerAddEvent(vk1.hex())
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(events.ObjectCreateEvent(o1v0))            
        machine.process_event(oce)

        s1v0 = self.standard_schema()
        sce = signer.sign(events.ObjectCreateEvent(s1v0))            
        machine.process_event(sce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(
            events.AnnotationCreateEvent([o1v0.identifier()], a1v0))
        machine.process_event(ace)

        ade = signer.sign(events.AnnotationDeleteEvent(a1v0.identifier()))    
        machine.process_event(ade)

        rre = events.ReviewRejectEvent(ade.uuid)
        rre = signer.sign(rre)
        machine.process_event(rre)

        con = sqlite3.connect(state.database_path)
        self.closers.append(con)
        cur = con.cursor()
        cur.execute("""SELECT status
                FROM annotation_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(a1v0.uuid), a1v0.version))

        status = set([getattr(events.StatusT, ea) for ea, in cur.fetchall()])

        self.assertTrue(events.StatusT.CREATE_PENDING in status)
        self.assertEqual(len(status), 1)

if __name__ == '__main__':
    unittest.main()