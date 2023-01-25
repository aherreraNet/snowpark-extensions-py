"""Provides Additional Extensions for Snowpark"""


from .dataframe_extensions import *
from .functions_extensions import *
from .session_builder_extensions import *
from .types_extensions import *
from .column_extensions import *
from .utils import display

def load_ipython_extension(ipython):
    def instructions():
        inst = """
import pandas as pd
from snowflake.snowpark import Session
import snowpark_extensions
from snowflake.snowpark.functions import col, lit
from snowflake.snowpark import functions as F
        """
        return inst 
    error_message_template="""<div style="background-color: #f44336; color: white; padding: 16px;"><strong>Error:</strong> <span id="error-message">@error</span></div>"""
    output_cell_output = None
    try:
        output_cell_output = displayHTML
    except:
        from IPython.display import display, HTML
        output_cell_output = lambda x: display(HTML(x))
    from IPython.core.magic import (Magics, magics_class, cell_magic)
    @magics_class
    class SnowparkMagics(Magics):
        def __init__(self, shell):
            super(SnowparkMagics, self).__init__(shell)
        @cell_magic
        def sql(self, line, cell):
            if "session" in self.shell.user_ns:
                session = self.shell.user_ns['session']
                from jinja2 import Template
                t = Template(cell)
                res = t.render(self.shell.user_ns)
                name = None
                if line and line.strip():
                    name = line.strip().split(" ")[0]
                from snowflake.snowpark.exceptions import SnowparkSQLException
                try:
                    df = session.sql(res)
                    html = df.to_pandas().to_html()
                    output_cell_output(html)
                    if name:
                        self.shell.user_ns[name] = df
                    else:
                        self.shell.user_ns["$df"] = df
                except SnowparkSQLException as sce:
                    error_msg = sce.message
                    formatted = error_message_template.replace("@error", error_msg)
                    output_cell_output(formatted)
                except Exception as ex:
                    error_message = str(ex)
                    output_cell_output(f"<pre>{error_message}</pre>")                    
            else:
                return "No session was found. You can setup one by running: session = Session.builder.from_env().getOrCreate()"
    magics = SnowparkMagics(ipython)
    ipython.register_magics(magics)
    compiled_file = "prep_log.py"
    code = compile(instructions(), compiled_file, 'exec')
    async def main():
        await ipython.run_code(code,{})
    import nest_asyncio
    nest_asyncio.apply()
    import asyncio
    asyncio.get_event_loop().run_until_complete(main())

