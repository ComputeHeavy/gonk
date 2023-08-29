import core
import memstate
import memrk
import unittest

class TestState(unittest.TestCase):
    def test_machine_register(self):
        state = memstate.State()
        machine = core.Machine(memrk.RecordKeeper())
        machine.register(state)

        self.assertEqual(len(machine.validators), 1)
        self.assertEqual(machine.validators[0], state)
        self.assertEqual(len(machine.consumers), 1)
        self.assertEqual(machine.consumers[0], state)

    def test_object_create(self):
        state = memstate.State()
        machine = core.Machine(memrk.RecordKeeper())
        machine.register(state)

        o1v0 = core.Object("object-1")
        machine.process_event(core.ObjectCreateEvent(o1v0))

        self.assertEqual(len(state.objects), 1)
        self.assertEqual(len(state.objects[o1v0.uuid]), 1)
        self.assertEqual(state.objects[o1v0.uuid][0], o1v0)

    def test_object_update(self):
        state = memstate.State()
        machine = core.Machine(memrk.RecordKeeper())
        machine.register(state)

        o1v0 = core.Object("object-1")
        machine.process_event(core.ObjectCreateEvent(o1v0))

        o1v1 = o1v0.versioned_copy()
        machine.process_event(core.ObjectUpdateEvent(o1v1))

        self.assertEqual(len(state.objects), 1)
        self.assertEqual(len(state.objects[o1v0.uuid]), 2)
        self.assertEqual(state.objects[o1v0.uuid][0], o1v0)
        self.assertEqual(state.objects[o1v0.uuid][1], o1v1)

    def test_object_delete(self):
        state = memstate.State()
        machine = core.Machine(memrk.RecordKeeper())
        machine.register(state)

        o1v0 = core.Object("object-1")
        machine.process_event(core.ObjectCreateEvent(o1v0))

        machine.process_event(core.ObjectDeleteEvent(o1v0.identifier()))

        self.assertEqual(len(state.objects), 1)
        self.assertEqual(len(state.objects[o1v0.uuid]), 1)
        self.assertEqual(state.objects[o1v0.uuid][0], o1v0)
        self.assertTrue(o1v0.identifier() in state.deleted_objects)

    def test_annotation_create(self):
        state = memstate.State()
        machine = core.Machine(memrk.RecordKeeper())
        machine.register(state)

        o1v0 = core.Object("object-1")
        machine.process_event(core.ObjectCreateEvent(o1v0))

        a1v0 = core.Annotation()
        machine.process_event(
            core.AnnotationCreateEvent([o1v0.identifier()], a1v0))

        self.assertEqual(len(state.annotations), 1)
        self.assertEqual(len(state.annotations[a1v0.uuid]), 1)
        self.assertEqual(state.annotations[a1v0.uuid][0], a1v0)

        self.assertEqual(len(state.link.forward), 1)
        self.assertEqual(len(state.link.forward[o1v0.identifier()]), 1)
        self.assertEqual(state.link.forward[o1v0.identifier()][0], a1v0.uuid)

        self.assertEqual(len(state.link.reverse), 1)
        self.assertEqual(len(state.link.reverse[a1v0.uuid]), 1)
        self.assertEqual(state.link.reverse[a1v0.uuid][0], o1v0.identifier())

    def test_annotation_update(self):
        state = memstate.State()
        machine = core.Machine(memrk.RecordKeeper())
        machine.register(state)

        o1v0 = core.Object("object-1")
        machine.process_event(core.ObjectCreateEvent(o1v0))

        a1v0 = core.Annotation()
        machine.process_event(
            core.AnnotationCreateEvent([o1v0.identifier()], a1v0))

        a1v1 = a1v0.versioned_copy()
        machine.process_event(core.AnnotationUpdateEvent(a1v1))

        self.assertEqual(len(state.annotations), 1)
        self.assertEqual(len(state.annotations[a1v0.uuid]), 2)
        self.assertEqual(state.annotations[a1v0.uuid][0], a1v0)
        self.assertEqual(state.annotations[a1v0.uuid][1], a1v1)

    def test_annotation_delete(self):
        state = memstate.State()
        machine = core.Machine(memrk.RecordKeeper())
        machine.register(state)

        o1v0 = core.Object("object-1")
        machine.process_event(core.ObjectCreateEvent(o1v0))

        a1v0 = core.Annotation()
        machine.process_event(
            core.AnnotationCreateEvent([o1v0.identifier()], a1v0))

        machine.process_event(core.AnnotationDeleteEvent(a1v0.identifier()))

        self.assertEqual(len(state.annotations), 1)
        self.assertEqual(len(state.annotations[a1v0.uuid]), 1)
        self.assertEqual(state.annotations[a1v0.uuid][0], a1v0)
        self.assertTrue(a1v0.identifier() in state.deleted_annotations)

if __name__ == '__main__':
    unittest.main()