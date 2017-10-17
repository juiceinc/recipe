# import sys
#
# import gevent
# from gevent.queue import JoinableQueue, Queue
# from recipe.redshift_connectionbase import redshift_create_engine_green
# from sqlalchemy.orm import sessionmaker
#
# from recipe import caching_query
# from recipe.connectionbase import regions, debug_queries, init_session
#
#
# class RecipePool(object):
#     def __init__(self, recipes,
#          silence_error=settings.RECIPEPOOL_SILENT_FAIL):
#         self.recipes = recipes
#         self.POOL_MAX = 5
#         self.tasks = JoinableQueue()
#         self.output_queue = Queue()
#         self.error = None
#         self.running = []
#         self.silence_error = silence_error
#         target = getattr(self.recipes[0][0].service, 'database', 'juicebox')
#         self.engine = redshift_create_engine_green(target=target)
#         debug_queries(self.engine)
#         self.engine.connect()
#
#         if getattr(settings, 'ALLOW_QUERY_CACHING', True):
#             self.Session = sessionmaker(
#                 bind=self.engine, autoflush=False, autocommit=False,
#                 query_cls=caching_query.query_callable(regions)
#             )
#         else:
#             self.Session = init_session()
#
#     def __query(self, recipe, name, flavor, render_config):
#         recipe.session(self.Session())
#         result = recipe.render(name, flavor=flavor,
#                                render_config=render_config)
#         return result
#
#     def executor(self):
#         while not self.tasks.empty():
#             task = self.tasks.get()
#             render_config = None
#             flavor = None
#             if len(task) == 4:
#                 recipe, name, flavor, render_config = task
#             elif len(task) == 3:
#                 recipe, name, flavor = task
#             else:
#                 recipe, name = task
#             try:
#                 results = self.__query(recipe, name, flavor, render_config)
#                 self.output_queue.put_nowait((results, name))
#                 self.tasks.task_done()
#             except:
#                 self.tasks.task_done()
#                 self.error = sys.exc_info()
#
#     def overseer(self):
#         for recipe in self.recipes:
#             self.tasks.put_nowait(recipe)
#
#     def run(self):
#         gevent.spawn(self.overseer).join()
#         for i in range(self.POOL_MAX):
#             runner = gevent.spawn(self.executor)
#             runner.start()
#             self.running.append(runner)
#
#         self.tasks.join()
#         for runner in self.running:
#             runner.kill()
#         if self.error and not self.silence_error:
#             raise self.error[0], self.error[1], self.error[2]
#         output = {}
#         for x in xrange(len(self.output_queue)):
#             result, name = self.output_queue.get()
#             output[name] = result
#         results = []
#         for recipe in self.recipes:
#             if len(recipe) == 4:
#                 _, name, _, _ = recipe
#             elif len(recipe) == 3:
#                 _, name, _ = recipe
#             else:
#                 _, name = recipe
#             if name in output:
#                 results.append(output[name])
#
#         return results
